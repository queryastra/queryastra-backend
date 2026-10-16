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

_BASE_SYSTEM_PROMPT = (
    "You are a senior DBA specializing in database performance. "
    "You have received a live diagnostic snapshot from the monitored instance. "
    "Produce a THOROUGH and ACTIONABLE analysis based exclusively on the provided data. "
    "Identify ALL significant problems visible in the data — do not limit yourself to a small number. "
    "For EACH problem: (1) name it precisely, (2) cite the specific rows/values that reveal it, "
    "(3) explain the root cause and production impact in detail, "
    "(4) provide concrete, step-by-step remediation commands or scripts where applicable. "
    "Be as specific as possible: quote table names, index names, query text, and numeric values. "
    "Never give generic advice — every recommendation must be grounded in the actual data provided. "
    "If a data section is empty, skip it entirely without comment. "
    "Respond in {language}, using Markdown with one ## section per problem, "
    "and a final ## Summary section ranking the top 3 actions by urgency."
)


def _build_system_prompt(locale: str) -> str:
    language = _LOCALE_TO_LANGUAGE.get(locale, "English")
    return _BASE_SYSTEM_PROMPT.format(language=language)


@dataclass
class DeepAnalysisResult:
    configured: bool
    narrative: str | None = None
    data_collected: list[str] = field(default_factory=list)
    tokens_consumed: int = 0


class DeepAnalysisService:
    def __init__(self, advisor: AiAdvisor | MultiKeyAiAdvisor) -> None:
        self._advisor = advisor

    async def run(self, collector: object, instance: object, locale: str = "en") -> DeepAnalysisResult:
        """Coleta diagnóstico da instância e solicita análise à IA."""
        if not self._advisor.is_configured:
            return DeepAnalysisResult(configured=False)

        sections: list[str] = []
        data_collected: list[str] = []

        # Cabeçalho da instância
        sections.append(
            f"## Instance\n"
            f"- Engine: {instance.db_type}\n"
            f"- Version: {instance.engine_version or 'unknown'}\n"
            f"- Criticality: {instance.criticality}\n"
        )

        # 1. Índices não utilizados
        try:
            unused = await collector.collect_unused_indexes()
            if unused:
                data_collected.append("unused_indexes")
                rows = sorted(unused, key=lambda x: x.index_size_bytes, reverse=True)[:15]
                lines = ["## Unused Indexes (scans since last statistics reset)",
                         "| Schema | Table | Index | Size (MB) | Scans |",
                         "|--------|-------|-------|-----------|-------|"]
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
                lines = ["## Table I/O Stats (sorted by seq_scan — high = missing index candidate)",
                         "| Schema | Table | Seq Scans | Idx Scans | Live Tuples | Dead Tuples | Last VACUUM |",
                         "|--------|-------|-----------|-----------|-------------|-------------|-------------|"]
                for r in rows:
                    last_vac = r.last_autovacuum or r.last_vacuum
                    vac_label = last_vac.strftime("%Y-%m-%d") if last_vac else "never"
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
                lines = ["## Top Queries by Average Execution Time (cumulative since last reset)",
                         "| Query (truncated) | Calls | Avg Time (ms) | Total Time (s) |",
                         "|-------------------|-------|---------------|----------------|"]
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
                lines = ["## Missing Index Suggestions",
                         "| Table | Database | Suggestion | Impact Score |",
                         "|-------|----------|------------|--------------|"]
                for r in rows:
                    score = f"{r.impact_score:.0f}" if r.impact_score else "-"
                    suggestion = r.suggestion[:100].replace("|", "\\|")
                    lines.append(f"| {r.table_name} | {r.database_name or '—'} | {suggestion} | {score} |")
                sections.append("\n".join(lines))
        except Exception as exc:
            logger.debug("collect_missing_index_suggestions falhou: %s", exc)

        if not data_collected:
            return DeepAnalysisResult(
                configured=True,
                narrative=None,
                data_collected=[],
                tokens_consumed=0,
            )

        user_content = "\n\n".join(sections)
        result: AiAnalysis = await self._advisor.analyze_prompt(
            user_content=user_content,
            system_prompt=_build_system_prompt(locale),
            max_tokens=2500,
        )

        return DeepAnalysisResult(
            configured=True,
            narrative=result.narrative,
            data_collected=data_collected,
            tokens_consumed=result.tokens_consumed,
        )
