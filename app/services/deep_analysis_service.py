"""Serviço de análise profunda de instância via IA.

Coleta diagnóstico ao vivo usando os métodos do Collector (conexão direta ao banco monitorado)
e envia ao modelo de IA configurado para análise acionável. Nenhuma query nova é necessária —
reutiliza os métodos já existentes: collect_unused_indexes, collect_table_io_stats,
collect_query_stats_snapshot, collect_missing_index_suggestions.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.core.logging import get_logger
from app.services.ai_advisor import AiAdvisor, AiAnalysis, MultiKeyAiAdvisor

logger = get_logger(__name__)

_SYSTEM_PROMPT = (
    "Você é um DBA sênior especialista em performance de banco de dados. "
    "Você recebeu um snapshot de diagnóstico coletado AO VIVO diretamente da instância monitorada. "
    "Com base exclusivamente nos dados fornecidos, produza uma análise PROFUNDA e ACIONÁVEL. "
    "Identifique os 3 a 5 problemas mais críticos presentes nos dados. "
    "Para cada problema: (1) nomeie o problema claramente, (2) explique a causa raiz com base nos dados, "
    "(3) descreva o impacto em produção, (4) liste as ações concretas para corrigir. "
    "Use os valores numéricos dos dados para justificar cada achado — seja específico, não genérico. "
    "Se um tipo de dado estiver ausente ou vazio, ignore-o sem mencionar. "
    "Responda em português do Brasil, em formato Markdown com seções por problema."
)


@dataclass
class DeepAnalysisResult:
    configured: bool
    narrative: str | None = None
    data_collected: list[str] = field(default_factory=list)
    tokens_consumed: int = 0


class DeepAnalysisService:
    def __init__(self, advisor: AiAdvisor | MultiKeyAiAdvisor) -> None:
        self._advisor = advisor

    async def run(self, collector: object, instance: object) -> DeepAnalysisResult:
        """Coleta diagnóstico da instância e solicita análise à IA."""
        if not self._advisor.is_configured:
            return DeepAnalysisResult(configured=False)

        sections: list[str] = []
        data_collected: list[str] = []

        # Cabeçalho da instância
        sections.append(
            f"## Instância\n"
            f"- Engine: {instance.db_type}\n"
            f"- Versão: {instance.engine_version or 'desconhecida'}\n"
            f"- Criticidade: {instance.criticality}\n"
        )

        # 1. Índices não utilizados
        try:
            unused = await collector.collect_unused_indexes()
            if unused:
                data_collected.append("unused_indexes")
                rows = sorted(unused, key=lambda x: x.index_size_bytes, reverse=True)[:15]
                lines = ["## Índices Não Utilizados (scans desde último reset de estatísticas)",
                         "| Schema | Tabela | Índice | Tamanho (MB) | Scans |",
                         "|--------|--------|--------|-------------|-------|"]
                for r in rows:
                    mb = r.index_size_bytes / 1_048_576
                    lines.append(f"| {r.schema_name} | {r.table_name} | {r.index_name} | {mb:.1f} | {r.idx_scan} |")
                sections.append("\n".join(lines))
        except Exception as exc:
            logger.debug("collect_unused_indexes falhou: %s", exc)

        # 2. I/O de tabelas (seq scans altos indicam falta de índice; dead tuples indicam necessidade de VACUUM)
        try:
            table_io = await collector.collect_table_io_stats()
            if table_io:
                data_collected.append("table_io_stats")
                rows = sorted(table_io, key=lambda x: x.seq_scan, reverse=True)[:15]
                lines = ["## I/O de Tabelas (ordenado por seq_scan — alto = candidato a novo índice)",
                         "| Schema | Tabela | Seq Scans | Idx Scans | Live Tuples | Dead Tuples | Último VACUUM |",
                         "|--------|--------|-----------|-----------|-------------|-------------|---------------|"]
                for r in rows:
                    last_vac = r.last_autovacuum or r.last_vacuum
                    vac_label = last_vac.strftime("%Y-%m-%d") if last_vac else "nunca"
                    dead_pct = (r.n_dead_tup * 100 / max(r.n_live_tup + r.n_dead_tup, 1))
                    lines.append(
                        f"| {r.schema_name} | {r.table_name} | {r.seq_scan:,} | {r.idx_scan:,} "
                        f"| {r.n_live_tup:,} | {r.n_dead_tup:,} ({dead_pct:.0f}%) | {vac_label} |"
                    )
                sections.append("\n".join(lines))
        except Exception as exc:
            logger.debug("collect_table_io_stats falhou: %s", exc)

        # 3. Snapshot de query stats (acumulativo desde último reset)
        try:
            qstats = await collector.collect_query_stats_snapshot()
            if qstats:
                data_collected.append("query_stats_snapshot")
                rows = sorted(qstats, key=lambda x: (x.total_exec_time / max(x.calls, 1)), reverse=True)[:10]
                lines = ["## Top Queries por Tempo Médio de Execução (acumulativo desde último reset)",
                         "| Query (truncada) | Chamadas | Tempo Médio (ms) | Tempo Total (s) |",
                         "|------------------|----------|-----------------|-----------------|"]
                for r in rows:
                    avg_ms = r.total_exec_time / max(r.calls, 1)
                    total_s = r.total_exec_time / 1000
                    query_preview = (r.query_text or "")[:80].replace("\n", " ").replace("|", "\\|")
                    lines.append(f"| `{query_preview}` | {r.calls:,} | {avg_ms:.1f} | {total_s:.1f} |")
                sections.append("\n".join(lines))
        except Exception as exc:
            logger.debug("collect_query_stats_snapshot falhou: %s", exc)

        # 4. Sugestões de índice ausente (SQL Server e PostgreSQL)
        try:
            missing = await collector.collect_missing_index_suggestions()
            if missing:
                data_collected.append("missing_index_suggestions")
                rows = sorted(missing, key=lambda x: (x.impact_score or 0), reverse=True)[:10]
                lines = ["## Sugestões de Índice Ausente",
                         "| Tabela | Banco | Sugestão | Score de Impacto |",
                         "|--------|-------|----------|-----------------|"]
                for r in rows:
                    score = f"{r.impact_score:.0f}" if r.impact_score else "—"
                    suggestion = r.suggestion[:100].replace("|", "\\|")
                    lines.append(f"| {r.table_name} | {r.database_name or '—'} | {suggestion} | {score} |")
                sections.append("\n".join(lines))
        except Exception as exc:
            logger.debug("collect_missing_index_suggestions falhou: %s", exc)

        if not data_collected:
            # Nenhum dado disponível — engine não suporta nenhuma das queries ou está inacessível
            return DeepAnalysisResult(
                configured=True,
                narrative="Nenhum dado de diagnóstico disponível para este engine ou instância.",
                data_collected=[],
                tokens_consumed=0,
            )

        user_content = "\n\n".join(sections)
        result: AiAnalysis = await self._advisor.analyze_prompt(
            user_content=user_content,
            system_prompt=_SYSTEM_PROMPT,
            max_tokens=1500,
        )

        return DeepAnalysisResult(
            configured=True,
            narrative=result.narrative,
            data_collected=data_collected,
            tokens_consumed=result.tokens_consumed,
        )
