"""Diagnóstico contextual via IA — análise narrativa da query combinando SQL, métricas de
execução e achados dos advisors estáticos.

Suporte a múltiplos provedores via configuração (BYOK — Bring Your Own Key):
  - openai   → OpenAI GPT (padrão), Azure OpenAI ou qualquer endpoint compatível (Ollama, LM Studio)
  - anthropic → Anthropic Claude (Messages API)

Quando nenhuma chave está configurada, retorna `AiAnalysis(configured=False)` — o painel
aparece na UI com uma mensagem explicando como configurar, nunca um erro.

Multi-key failover: `MultiKeyAiAdvisor` tenta as chaves habilitadas em ordem de prioridade,
pulando as que falharam nos últimos 5 minutos (cooldown via `last_error_at`). Após a chamada
bem-sucedida, atualiza `tokens_used` e `last_used_at` no banco.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

from app.core.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",
    "anthropic": "claude-haiku-5-5",
}

_LOCALE_TO_LANGUAGE: dict[str, str] = {
    "pt-BR": "Brazilian Portuguese",
    "pt-PT": "European Portuguese",
    "en": "English",
    "es": "Spanish",
    "de": "German",
    "fr": "French",
    "it": "Italian",
    "nl": "Dutch",
    "ja": "Japanese",
    "ko": "Korean",
    "zh-CN": "Simplified Chinese",
}

_BASE_QUERY_SYSTEM_PROMPT = (
    "You are a senior DBA specializing in database performance. "
    "Analyze the running SQL statement and, when available, the full source of the object "
    "(batch/procedure/function) it belongs to. Use the full context to identify logic, "
    "structural, and performance problems in the object as a whole, not just the isolated statement. "
    "Produce a CONCISE and ACTIONABLE diagnosis in up to 4 short paragraphs. "
    "Focus on: (1) likely root cause, (2) observed impact, (3) the 2-3 most important actions to take. "
    "Do not list each finding individually — synthesize a narrative. "
    "Respond in {language}. Be direct, with no generic introductions."
)


def _build_query_system_prompt(locale: str) -> str:
    language = _LOCALE_TO_LANGUAGE.get(locale, "English")
    return _BASE_QUERY_SYSTEM_PROMPT.format(language=language)


@dataclass(frozen=True, slots=True)
class AiAnalysis:
    configured: bool
    narrative: str | None = None
    tokens_consumed: int = 0


class AiAdvisor:
    """Chama um único provedor de IA e retorna narrativa diagnóstica.

    `configured=False` em qualquer falha de configuração — nunca propaga exceção.
    """

    def __init__(
        self,
        provider: str | None,
        api_key: str | None,
        base_url: str | None,
        model: str | None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._provider = (provider or "openai").lower()
        self._api_key = api_key
        self._base_url = (base_url or "").rstrip("/") or None
        self._model = model or _DEFAULT_MODELS.get(self._provider, "gpt-4o-mini")
        self._timeout = timeout_seconds

    @property
    def is_configured(self) -> bool:
        # Ollama não precisa de chave (self-hosted)
        if self._provider == "openai" and self._base_url and not self._api_key:
            return True
        return bool(self._api_key)

    async def analyze(
        self,
        sql_text_masked: str,
        db_type: str,
        avg_duration_ms: float | None,
        sql_findings: list[dict],
        plan_findings: list[dict],
        object_text_masked: str | None = None,
        locale: str = "en",
    ) -> AiAnalysis:
        if not self.is_configured:
            return AiAnalysis(configured=False)

        system_prompt = _build_query_system_prompt(locale)
        user_content = _build_user_prompt(
            sql_text_masked, db_type, avg_duration_ms, sql_findings, plan_findings,
            object_text_masked=object_text_masked,
        )
        try:
            if self._provider == "anthropic":
                narrative, tokens = await self._call_anthropic(user_content, system_prompt=system_prompt)
            else:
                narrative, tokens = await self._call_openai_compat(user_content, system_prompt=system_prompt)
        except httpx.HTTPStatusError as exc:
            body = exc.response.text[:300] if exc.response.content else "(sem corpo)"
            logger.warning(
                "Falha ao consultar IA (%s %s model=%s): HTTP %d — %s",
                self._provider,
                self._base_url or "(default)",
                self._model,
                exc.response.status_code,
                body,
            )
            return AiAnalysis(configured=True, narrative=None)
        except Exception as exc:
            logger.warning(
                "Falha ao consultar IA (%s %s model=%s): %s",
                self._provider,
                self._base_url or "(default)",
                self._model,
                exc,
            )
            return AiAnalysis(configured=True, narrative=None)

        return AiAnalysis(configured=True, narrative=narrative, tokens_consumed=tokens)

    async def analyze_prompt(
        self,
        user_content: str,
        system_prompt: str,
        max_tokens: int = 1200,
    ) -> AiAnalysis:
        """Chama o modelo com system_prompt e user_content arbitrários.

        Usado pela análise profunda de instância, que tem sistema e conteúdo diferentes
        do advisor de queries. Retorna AiAnalysis com configured=False se não houver chave.
        """
        if not self.is_configured:
            return AiAnalysis(configured=False)
        try:
            if self._provider == "anthropic":
                narrative, tokens = await self._call_anthropic(user_content, system_prompt=system_prompt, max_tokens=max_tokens)
            else:
                narrative, tokens = await self._call_openai_compat(user_content, system_prompt=system_prompt, max_tokens=max_tokens)
        except Exception as exc:
            logger.warning("Falha em analyze_prompt (%s): %s", self._provider, exc)
            return AiAnalysis(configured=True, narrative=None)
        return AiAnalysis(configured=True, narrative=narrative, tokens_consumed=tokens)

    async def _call_openai_compat(
        self,
        user_content: str,
        system_prompt: str = _SYSTEM_PROMPT,
        max_tokens: int = 600,
    ) -> tuple[str | None, int]:
        base = self._base_url or "https://api.openai.com"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(f"{base}/v1/chat/completions", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        narrative = data["choices"][0]["message"]["content"].strip()
        tokens = data.get("usage", {}).get("total_tokens", 0)
        return narrative, tokens

    async def _call_anthropic(
        self,
        user_content: str,
        system_prompt: str = _SYSTEM_PROMPT,
        max_tokens: int = 600,
    ) -> tuple[str | None, int]:
        base = self._base_url or "https://api.anthropic.com"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self._api_key or "",
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": self._model,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_content}],
            "max_tokens": max_tokens,
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(f"{base}/v1/messages", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        narrative = data["content"][0]["text"].strip()
        usage = data.get("usage", {})
        tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
        return narrative, tokens


class MultiKeyAiAdvisor:
    """Failover automático entre múltiplas chaves de API armazenadas no banco.

    Tenta as chaves habilitadas em ordem de prioridade. Pula chaves que falharam
    nos últimos 5 minutos (via `last_error_at`). Atualiza contadores de token após
    cada chamada bem-sucedida.
    """

    def __init__(self, key_advisors: list[tuple]) -> None:
        # Lista de (AiApiKey, AiApiKeyRepository, AiAdvisor)
        self._key_advisors = key_advisors

    @property
    def is_configured(self) -> bool:
        return bool(self._key_advisors)

    async def analyze(
        self,
        sql_text_masked: str,
        db_type: str,
        avg_duration_ms: float | None,
        sql_findings: list[dict],
        plan_findings: list[dict],
        object_text_masked: str | None = None,
        locale: str = "en",
    ) -> AiAnalysis:
        if not self._key_advisors:
            return AiAnalysis(configured=False)

        from app.repositories.ai_api_key_repository import AiApiKeyRepository

        system_prompt = _build_query_system_prompt(locale)
        for row, repo, advisor in self._key_advisors:
            if AiApiKeyRepository.is_in_error_cooldown(row):
                logger.warning("Pulando chave '%s' (cooldown ativo, last_error_at=%s)", row.name, row.last_error_at)
                continue
            if row.token_limit and row.tokens_used >= row.token_limit:
                logger.warning(
                    "Pulando chave '%s' (limite de tokens atingido: %d/%d)",
                    row.name, row.tokens_used, row.token_limit,
                )
                continue
            logger.warning("Tentando chave '%s' (base_url=%s, model=%s)", row.name, advisor._base_url, advisor._model)

            user_content = _build_user_prompt(
                sql_text_masked, db_type, avg_duration_ms, sql_findings, plan_findings,
                object_text_masked=object_text_masked,
            )
            try:
                if advisor._provider == "anthropic":
                    narrative, tokens = await advisor._call_anthropic(user_content, system_prompt=system_prompt)
                else:
                    narrative, tokens = await advisor._call_openai_compat(user_content, system_prompt=system_prompt)

                await repo.record_success(row, tokens)
                return AiAnalysis(configured=True, narrative=narrative, tokens_consumed=tokens)

            except httpx.HTTPStatusError as exc:
                body = exc.response.text[:300] if exc.response.content else "(sem corpo)"
                logger.warning(
                    "Chave '%s' retornou HTTP %d — tentando próxima. Corpo: %s",
                    row.name,
                    exc.response.status_code,
                    body,
                )
                await repo.record_error(row)

            except Exception as exc:
                logger.warning("Erro inesperado na chave '%s' — tentando próxima: %s", row.name, exc)
                await repo.record_error(row)

        logger.warning("Todas as chaves de IA falharam ou estão em cooldown")
        return AiAnalysis(configured=True, narrative=None)

    async def analyze_prompt(
        self,
        user_content: str,
        system_prompt: str,
        max_tokens: int = 1200,
    ) -> AiAnalysis:
        """Failover multi-key com system_prompt e conteúdo arbitrários."""
        if not self._key_advisors:
            return AiAnalysis(configured=False)

        from app.repositories.ai_api_key_repository import AiApiKeyRepository

        for row, repo, advisor in self._key_advisors:
            if AiApiKeyRepository.is_in_error_cooldown(row):
                continue
            if row.token_limit and row.tokens_used >= row.token_limit:
                continue
            try:
                if advisor._provider == "anthropic":
                    narrative, tokens = await advisor._call_anthropic(user_content, system_prompt=system_prompt, max_tokens=max_tokens)
                else:
                    narrative, tokens = await advisor._call_openai_compat(user_content, system_prompt=system_prompt, max_tokens=max_tokens)
                await repo.record_success(row, tokens)
                return AiAnalysis(configured=True, narrative=narrative, tokens_consumed=tokens)
            except Exception as exc:
                logger.warning("Chave '%s' falhou em analyze_prompt: %s", row.name, exc)
                await repo.record_error(row)

        return AiAnalysis(configured=True, narrative=None)


def _build_user_prompt(
    sql_text: str,
    db_type: str,
    avg_duration_ms: float | None,
    sql_findings: list[dict],
    plan_findings: list[dict],
    object_text_masked: str | None = None,
) -> str:
    lines = [f"## Contexto\n- Banco: {db_type}"]
    if avg_duration_ms is not None:
        lines.append(f"- Duração média: {avg_duration_ms:.0f} ms")

    if sql_findings:
        lines.append("\n## Achados estáticos (regras de código)")
        for f in sql_findings[:8]:  # limita para não exceder tokens
            lines.append(f"- [{f.get('rule_id')} {f.get('severity')}] {f.get('description')}")

    if plan_findings:
        lines.append("\n## Achados do plano de execução")
        for f in plan_findings[:6]:
            op = f.get("operator") or ""
            obj = f.get("object_name") or ""
            suffix = f" ({op}{' — ' + obj if obj else ''})" if op else ""
            lines.append(f"- [{f.get('finding_id')} {f.get('severity')}]{suffix} {f.get('description')}")

    lines.append(f"\n## Statement em execução (valores mascarados)\n```sql\n{sql_text[:2000]}\n```")

    if object_text_masked and object_text_masked.strip() != sql_text.strip():
        lines.append(f"\n## Código completo do objeto (batch/procedure, valores mascarados)\n```sql\n{object_text_masked[:4000]}\n```")

    return "\n".join(lines)
