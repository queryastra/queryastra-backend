"""Geracao de PDF para relatorios agendados (Item 4).

Tres tipos de relatorio:
  - top_sql: top N queries por wait time total no periodo
  - average_wait: wait medio por banco de dados no periodo
  - typical_day: media de wait por hora do dia (0-23) -- padrao de carga ao longo do dia
"""

from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pathlib

import sqlalchemy as sa
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image as RLImage,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

_ASSETS_DIR = pathlib.Path(__file__).parent.parent / "assets"
_LOGO_PNG = _ASSETS_DIR / "logo-planet-128.png"
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance

_STYLES = getSampleStyleSheet()
_TITLE_STYLE = ParagraphStyle(
    "ReportTitle",
    parent=_STYLES["Heading1"],
    fontSize=16,
    spaceAfter=4,
)
_SUBTITLE_STYLE = ParagraphStyle(
    "ReportSubtitle",
    parent=_STYLES["Normal"],
    fontSize=10,
    textColor=colors.HexColor("#666666"),
    spaceAfter=12,
)
_SECTION_STYLE = ParagraphStyle(
    "SectionHeader",
    parent=_STYLES["Heading2"],
    fontSize=12,
    spaceBefore=16,
    spaceAfter=6,
)
_CAPTION_STYLE = ParagraphStyle(
    "Caption",
    parent=_STYLES["Normal"],
    fontSize=8,
    textColor=colors.HexColor("#888888"),
    spaceAfter=4,
)
_SQL_CELL_STYLE = ParagraphStyle(
    "SqlCell",
    parent=_STYLES["Normal"],
    fontSize=8,
    fontName="Helvetica-Bold",
    leading=11,
)
_SQL_DETAIL_ID_STYLE = ParagraphStyle(
    "SqlDetailId",
    parent=_STYLES["Normal"],
    fontSize=9,
    fontName="Helvetica-Bold",
    spaceBefore=8,
    spaceAfter=2,
)
_SQL_DETAIL_META_STYLE = ParagraphStyle(
    "SqlDetailMeta",
    parent=_STYLES["Normal"],
    fontSize=8,
    textColor=colors.HexColor("#374151"),
    spaceAfter=3,
)
_SQL_DETAIL_CMD_STYLE = ParagraphStyle(
    "SqlDetailCmd",
    parent=_STYLES["Normal"],
    fontSize=7,
    fontName="Courier",
    leading=9,
    textColor=colors.HexColor("#1e3a5f"),
    backColor=colors.HexColor("#f1f5f9"),
    spaceAfter=6,
    leftIndent=6,
    rightIndent=6,
    borderPad=4,
)

_TABLE_HEADER_COLOR = colors.HexColor("#1e40af")
_TABLE_ALT_COLOR = colors.HexColor("#f0f4ff")

_COMMON_TABLE_STYLE = TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), _TABLE_HEADER_COLOR),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _TABLE_ALT_COLOR]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]
)


_LOGO_PURPLE = colors.HexColor("#2B1760")

_BRAND_STYLE = ParagraphStyle(
    "Brand",
    parent=_STYLES["Normal"],
    fontSize=18,
    fontName="Helvetica-Bold",
    textColor=_LOGO_PURPLE,
    leading=20,
)


def _build_header_table(report_title: str) -> Table:
    """Page header: QueryAstra logo+name on left, report title on right."""
    logo_size = 1.1 * cm
    if _LOGO_PNG.exists():
        logo: object = RLImage(str(_LOGO_PNG), width=logo_size, height=logo_size)
    else:
        logo = Paragraph("Q", _BRAND_STYLE)

    brand_cell = Table(
        [[logo, Paragraph("QueryAstra", _BRAND_STYLE)]],
        colWidths=[logo_size + 0.1 * cm, 5 * cm],
    )
    brand_cell.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    title_para = Paragraph(report_title, _TITLE_STYLE)

    header = Table(
        [[brand_cell, title_para]],
        colWidths=[6.5 * cm, 10.5 * cm],
    )
    header.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LINEBELOW", (0, 0), (-1, 0), 1, _LOGO_PURPLE),
    ]))
    return header


def _ms_to_readable(ms: float | None) -> str:
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms:.0f} ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f} s"
    return f"{ms / 60_000:.1f} min"


def _pct_bar(pct: int, width: int = 20) -> str:
    """Barra de progresso ASCII simples (sem caracteres Unicode especiais)."""
    filled = pct * width // 100
    return "[" + "#" * filled + "-" * (width - filled) + f"] {pct}%"


def _sql_cell(sql_id: str) -> Paragraph:
    """Celula de query: apenas o ID em negrito (o detalhe fica abaixo da tabela)."""
    return Paragraph(sql_id, _SQL_CELL_STYLE)


def _sql_detail_blocks(rank: int, sql_id: str, row: dict[str, Any]) -> list[Any]:
    """Detail blocks for a query (section below the summary table)."""
    blocks: list[Any] = []
    meta = (
        f"Database: <b>{str(row.get('database_name', 'N/A'))[:50]}</b>"
        f"  |  Executions: <b>{row.get('execution_count', 0)}</b>"
        f"  |  Total Wait: <b>{_ms_to_readable(row.get('total_wait_ms'))}</b>"
        f"  |  Dominant Wait: <b>{str(row.get('dominant_wait') or 'CPU')[:30]}</b>"
    )
    blocks.append(Paragraph(f"{rank}. {sql_id}", _SQL_DETAIL_ID_STYLE))
    blocks.append(Paragraph(meta, _SQL_DETAIL_META_STYLE))
    sql_text: str | None = row.get("sql_text")
    if sql_text:
        truncated = sql_text.strip()[:400]
        if len(sql_text.strip()) > 400:
            truncated += "..."
        safe = truncated.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        blocks.append(Paragraph(safe, _SQL_DETAIL_CMD_STYLE))
    else:
        blocks.append(Paragraph("(text not available)", _SQL_DETAIL_META_STYLE))
    return blocks


async def _query_top_sql(
    session: AsyncSession,
    instance_id: uuid.UUID,
    since: datetime,
    until: datetime,
    top_n: int,
) -> list[dict[str, Any]]:
    sql = sa.text("""
        SELECT
            sql_hash,
            MAX(sql_text_masked)        AS sql_text,
            COALESCE(MAX(database_name), 'N/A') AS database_name,
            COUNT(*)                    AS execution_count,
            SUM(wait_time_ms)           AS total_wait_ms,
            MODE() WITHIN GROUP (ORDER BY COALESCE(wait_event_native, 'CPU')) AS dominant_wait
        FROM query_samples
        WHERE instance_id = :instance_id
          AND time >= :since
          AND time <= :until
          AND session_id IS NOT NULL
        GROUP BY sql_hash
        ORDER BY total_wait_ms DESC
        LIMIT :top_n
    """)
    result = await session.execute(
        sql,
        {
            "instance_id": str(instance_id),
            "since": since,
            "until": until,
            "top_n": top_n,
        },
    )
    return [dict(row._mapping) for row in result.fetchall()]


async def _query_average_wait(
    session: AsyncSession,
    instance_id: uuid.UUID,
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    sql = sa.text("""
        SELECT
            COALESCE(database_name, 'N/A') AS database_name,
            COUNT(*)                        AS sample_count,
            SUM(wait_time_ms)               AS total_wait_ms,
            AVG(wait_time_ms)               AS avg_wait_ms,
            MODE() WITHIN GROUP (ORDER BY COALESCE(wait_event_native, 'CPU')) AS dominant_wait
        FROM query_samples
        WHERE instance_id = :instance_id
          AND time >= :since
          AND time <= :until
          AND session_id IS NOT NULL
        GROUP BY database_name
        ORDER BY total_wait_ms DESC
    """)
    result = await session.execute(
        sql,
        {
            "instance_id": str(instance_id),
            "since": since,
            "until": until,
        },
    )
    return [dict(row._mapping) for row in result.fetchall()]


async def _query_typical_day(
    session: AsyncSession,
    instance_id: uuid.UUID,
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    sql = sa.text("""
        SELECT
            EXTRACT(HOUR FROM time AT TIME ZONE 'UTC')::int AS hour_of_day,
            COUNT(*)                                         AS sample_count,
            AVG(wait_time_ms)                               AS avg_wait_ms,
            SUM(wait_time_ms)                               AS total_wait_ms
        FROM query_samples
        WHERE instance_id = :instance_id
          AND time >= :since
          AND time <= :until
          AND session_id IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """)
    result = await session.execute(
        sql,
        {
            "instance_id": str(instance_id),
            "since": since,
            "until": until,
        },
    )
    return [dict(row._mapping) for row in result.fetchall()]


def _build_pdf(
    title: str,
    instance: Instance,
    report_type: str,
    since: datetime,
    until: datetime,
    rows: list[dict[str, Any]],
) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    story: list[Any] = []

    # Header: QueryAstra logo + report title
    story.append(_build_header_table(title))
    story.append(Spacer(1, 0.3 * cm))

    since_str = since.strftime("%Y-%m-%d %H:%M")
    until_str = until.strftime("%Y-%m-%d %H:%M")
    period_str = f"{since_str} — {until_str} UTC"
    story.append(
        Paragraph(
            f"Instance: <b>{instance.name}</b> ({instance.host}) &nbsp;|&nbsp; "
            f"Period: {period_str}",
            _SUBTITLE_STYLE,
        )
    )
    story.append(
        Paragraph(
            f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC",
            _CAPTION_STYLE,
        )
    )
    story.append(Spacer(1, 0.4 * cm))

    if not rows:
        story.append(
            Paragraph("No data found for the specified period.", _STYLES["Normal"])
        )
    elif report_type == "top_sql":
        story.append(Paragraph("Top SQL — Queries with Highest Wait Time", _SECTION_STYLE))
        header_style = ParagraphStyle(
            "TblHdr", parent=_STYLES["Normal"],
            fontSize=9, fontName="Helvetica-Bold", textColor=colors.white,
        )
        data: list[list[Any]] = [[
            Paragraph("#", header_style),
            Paragraph("Query", header_style),
            Paragraph("Database", header_style),
            Paragraph("Total Wait", header_style),
            Paragraph("Executions", header_style),
            Paragraph("Dominant Wait", header_style),
        ]]
        sql_ids: list[str] = []
        for i, r in enumerate(rows, 1):
            sql_id = f"Q-{str(r['sql_hash'])[:8].upper()}"
            sql_ids.append(sql_id)
            data.append(
                [
                    str(i),
                    _sql_cell(sql_id),
                    str(r["database_name"])[:30],
                    _ms_to_readable(r["total_wait_ms"]),
                    str(r["execution_count"]),
                    str(r["dominant_wait"] or "CPU")[:25],
                ]
            )
        col_widths = [0.7 * cm, 3.8 * cm, 3.8 * cm, 1.9 * cm, 1.9 * cm, 2.9 * cm]
        t = Table(data, colWidths=col_widths, repeatRows=1)
        t.setStyle(_COMMON_TABLE_STYLE)
        story.append(t)

        story.append(Spacer(1, 0.5 * cm))
        story.append(Paragraph("Query Details", _SECTION_STYLE))
        for i, r in enumerate(rows, 1):
            for block in _sql_detail_blocks(i, sql_ids[i - 1], r):
                story.append(block)

    elif report_type == "average_wait":
        story.append(Paragraph("Average Wait by Database", _SECTION_STYLE))
        data = [
            ["Database", "Samples", "Total Wait", "Avg Wait / Sample", "Dominant Wait"]
        ]
        for r in rows:
            data.append(
                [
                    str(r["database_name"])[:35],
                    str(r["sample_count"]),
                    _ms_to_readable(r["total_wait_ms"]),
                    _ms_to_readable(r["avg_wait_ms"]),
                    str(r["dominant_wait"] or "CPU")[:25],
                ]
            )
        col_widths = [5.5 * cm, 2.2 * cm, 2.8 * cm, 3.2 * cm, 3.3 * cm]
        t = Table(data, colWidths=col_widths, repeatRows=1)
        t.setStyle(_COMMON_TABLE_STYLE)
        story.append(t)

    elif report_type == "typical_day":
        story.append(
            Paragraph("Typical Day — Wait Pattern by Hour of Day (UTC)", _SECTION_STYLE)
        )
        by_hour = {int(r["hour_of_day"]): r for r in rows}
        data = [["Hour (UTC)", "Samples", "Total Wait", "Avg Wait / Sample", "Load"]]
        max_total = max((r["total_wait_ms"] or 0 for r in rows), default=1)
        for h in range(24):
            hr: dict[str, Any] | None = by_hour.get(h)
            if hr:
                pct = int((hr["total_wait_ms"] or 0) / max(max_total, 1) * 100)
                data.append(
                    [
                        f"{h:02d}h",
                        str(hr["sample_count"]),
                        _ms_to_readable(hr["total_wait_ms"]),
                        _ms_to_readable(hr["avg_wait_ms"]),
                        _pct_bar(pct),
                    ]
                )
            else:
                data.append([f"{h:02d}h", "0", "-", "-", _pct_bar(0)])
        col_widths = [2 * cm, 2.2 * cm, 2.8 * cm, 3.2 * cm, 6.8 * cm]
        t = Table(data, colWidths=col_widths, repeatRows=1)
        t.setStyle(_COMMON_TABLE_STYLE)
        story.append(t)

    doc.build(story)
    return buf.getvalue()


async def generate_report(
    session: AsyncSession,
    report_type: str,
    instance: Instance,
    title: str,
    params: dict[str, Any],
    lookback_days: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> bytes:
    """Gera o PDF do relatorio e retorna os bytes brutos.

    `lookback_days` e mutuamente exclusivo com `since`/`until` -- so um dos dois deve ser
    fornecido. Se nenhum for fornecido, usa 7 dias como padrao.
    """
    if until is None:
        until = datetime.now(UTC)
    if since is None:
        days = lookback_days or params.get("lookback_days", 7)
        since = until - timedelta(days=int(days))

    top_n = int(params.get("top_n", 10))

    if report_type == "top_sql":
        rows = await _query_top_sql(session, instance.id, since, until, top_n)
    elif report_type == "average_wait":
        rows = await _query_average_wait(session, instance.id, since, until)
    elif report_type == "typical_day":
        rows = await _query_typical_day(session, instance.id, since, until)
    else:
        raise ValueError(f"unknown report_type: {report_type!r}")

    return _build_pdf(title, instance, report_type, since, until, rows)
