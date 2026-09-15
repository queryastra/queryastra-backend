"""Cliente HTTP fino para o AB SQL Insight (`C:\\IA\\ab-sql-rules-checker`) — Etapa 10/11.

AB SQL Insight e um sistema SEPARADO deste projeto: uma instancia ja roda em producao em
`http://autklinux4:9100/`, expondo `POST /check` -> `{"rules": [{"id", "description", "details"}]}`.
Sua "Etapa 1" (`analyze_sql()`) e um motor de regras 100% deterministico (sem IA), reaproveitado
tal qual pelos modulos 1-4 daquele sistema — aceita uma unica string de comando T-SQL (nao um
script inteiro), e a maioria das regras nao depende de valor literal, entao `sql_text_masked`
(ja mascarado por este projeto) e um input seguro.

Integracao via chamada HTTP simples (nao vendorizado/importado) — desacopla o ciclo de vida dos
dois sistemas. Qualquer falha (timeout, 5xx, servico desligado/`base_url` nao configurada) degrada
para `available=False` com lista vazia — nunca derruba a tela de detalhe de query que consome
isso (mesmo padrao de capacidade opcional ja usado no restante do projeto).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SqlInsightRule:
    rule_id: str
    description: str
    details: str | None


@dataclass(frozen=True, slots=True)
class SqlInsightAnalysis:
    available: bool
    rules: list[SqlInsightRule] = field(default_factory=list)


class SqlInsightClient:
    """`available=False` (lista vazia) sempre que `base_url` for `None`/vazia, ou a chamada
    falhar por qualquer motivo — nunca propaga a excecao pro chamador."""

    def __init__(
        self,
        base_url: str | None,
        timeout_seconds: float,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/") if base_url else None
        self._timeout_seconds = timeout_seconds
        # `transport` so e passado em teste (`httpx.MockTransport`) — em producao fica `None` e
        # `httpx.AsyncClient` usa o transporte real (rede de verdade).
        self._transport = transport

    async def check(self, sql_text_masked: str) -> SqlInsightAnalysis:
        if not self._base_url:
            return SqlInsightAnalysis(available=False)

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout_seconds, transport=self._transport
            ) as client:
                response = await client.post(
                    f"{self._base_url}/check", json={"sql": sql_text_masked}
                )
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError):
            logger.warning("Falha ao consultar AB SQL Insight - degradando para indisponivel")
            return SqlInsightAnalysis(available=False)

        rules_payload = payload.get("rules", []) if isinstance(payload, dict) else []
        rules = [
            SqlInsightRule(
                rule_id=str(rule.get("id", "")),
                description=str(rule.get("description", "")),
                details=rule.get("details"),
            )
            for rule in rules_payload
            if isinstance(rule, dict)
        ]
        return SqlInsightAnalysis(available=True, rules=rules)
