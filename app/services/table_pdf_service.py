"""PDF generico de tabela (exportacao das telas de manutencao: saude de indices, estatisticas,
missing indexes...).

O frontend manda exatamente o que esta filtrado/visivel na tela (titulo, subtitulo, colunas e
linhas ja formatadas como texto); aqui so diagramamos: A4 paisagem, cabecalho QueryAstra (logo +
titulo), subtitulo, timestamp de geracao, tabela com cabecalho repetido a cada pagina e numero de
pagina no rodape. Limites (`MAX_*`) protegem o servidor de payloads abusivos — a validacao de
tamanho fica no schema da API, `build_table_pdf` apenas trunca defensivamente.
"""

from __future__ import annotations

import io
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.services.report_service import _build_header_table

MAX_COLUMNS = 30
MAX_ROWS = 5000
MAX_CELL_CHARS = 2000
MAX_TITLE_CHARS = 200

_STYLES = getSampleStyleSheet()
_SUBTITLE_STYLE = ParagraphStyle(
    "TablePdfSubtitle",
    parent=_STYLES["Normal"],
    fontSize=9,
    textColor=colors.HexColor("#4b5563"),
    spaceBefore=6,
    spaceAfter=2,
)
_META_STYLE = ParagraphStyle(
    "TablePdfMeta",
    parent=_STYLES["Normal"],
    fontSize=8,
    textColor=colors.HexColor("#888888"),
    spaceAfter=8,
)
_HEADER_CELL_STYLE = ParagraphStyle(
    "TablePdfHeaderCell",
    parent=_STYLES["Normal"],
    fontSize=8,
    leading=10,
    fontName="Helvetica-Bold",
    textColor=colors.white,
)
_CELL_STYLE = ParagraphStyle(
    "TablePdfCell",
    parent=_STYLES["Normal"],
    fontSize=7,
    leading=9,
    wordWrap="CJK",  # quebra tambem identificadores longos sem espaco (nomes de indice, SQL)
)
_NUMERIC_CELL_STYLE = ParagraphStyle("TablePdfNumericCell", parent=_CELL_STYLE, alignment=2)

_TABLE_STYLE = TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e40af")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f0f4ff")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]
)

PAGE_SIZE = landscape(A4)
_MARGIN = 1.2 * cm


@dataclass(frozen=True, slots=True)
class TableColumn:
    key: str
    label: str
    align: str = "left"  # "left" | "right"
    weight: float = 1.0  # largura relativa


_CID_FONTS: dict[str, str] = {
    "ko": "HYGothic-Medium",
    "ja": "HeiseiKakuGo-W5",
    "zh": "STSong-Light",
}


def pick_font(texts: Sequence[str]) -> str | None:
    """Helvetica (padrao do reportlab) so cobre Latin-1/WinAnsi — titulo/colunas traduzidos para
    ja/ko/zh-CN sairiam como quadrados. Detecta o script e devolve uma fonte CID embutida do
    reportlab (sem arquivo de fonte) que cobre o texto, ou `None` para manter Helvetica."""
    has_hangul = has_kana = has_han = False
    for text in texts:
        for ch in text:
            code = ord(ch)
            if 0xAC00 <= code <= 0xD7AF or 0x1100 <= code <= 0x11FF or 0x3130 <= code <= 0x318F:
                has_hangul = True
            elif 0x3040 <= code <= 0x30FF:
                has_kana = True
            elif 0x4E00 <= code <= 0x9FFF or 0x3400 <= code <= 0x4DBF:
                has_han = True
    if has_hangul:
        key = "ko"
    elif has_kana:
        key = "ja"
    elif has_han:
        key = "zh"
    else:
        return None
    name = _CID_FONTS[key]
    if name not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(UnicodeCIDFont(name))
    return name


def _with_font(style: ParagraphStyle, font: str | None) -> ParagraphStyle:
    if font is None:
        return style
    return ParagraphStyle(f"{style.name}-{font}", parent=style, fontName=font)


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        text = f"{value:,.2f}"
    else:
        text = str(value)
    if len(text) > MAX_CELL_CHARS:
        text = text[: MAX_CELL_CHARS - 1] + "…"
    return text


def _column_widths(columns: Sequence[TableColumn], available: float) -> list[float]:
    total = sum(max(c.weight, 0.1) for c in columns) or 1.0
    return [available * max(c.weight, 0.1) / total for c in columns]


def _draw_footer(canvas: Any, doc: Any) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#888888"))
    canvas.drawRightString(PAGE_SIZE[0] - _MARGIN, 0.6 * cm, f"{doc.page}")
    canvas.drawString(_MARGIN, 0.6 * cm, "QueryAstra")
    canvas.restoreState()


def build_table_pdf(
    *,
    title: str,
    subtitle: str | None,
    columns: Sequence[TableColumn],
    rows: Sequence[dict[str, Any]],
    generated_at: datetime | None = None,
    generated_label: str = "Generated at",
    truncated_note: str | None = None,
) -> bytes:
    """Gera o PDF (bytes). `rows` sao dicts indexados por `TableColumn.key`."""
    if not columns:
        raise ValueError("at least one column is required")
    cols = list(columns)[:MAX_COLUMNS]
    data_rows = list(rows)[:MAX_ROWS]
    generated = generated_at or datetime.now(UTC)

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=PAGE_SIZE,
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_MARGIN,
        bottomMargin=_MARGIN,
        title=title[:MAX_TITLE_CHARS],
        author="QueryAstra",
    )
    available_width = PAGE_SIZE[0] - 2 * _MARGIN

    cell_texts = [[_cell_text(row.get(c.key)) for c in cols] for row in data_rows]
    font = pick_font(
        [title, subtitle or "", generated_label, truncated_note or ""]
        + [c.label for c in cols]
        + [t for r in cell_texts for t in r]
    )
    subtitle_style = _with_font(_SUBTITLE_STYLE, font)
    meta_style = _with_font(_META_STYLE, font)
    header_style = _with_font(_HEADER_CELL_STYLE, font)
    cell_style = _with_font(_CELL_STYLE, font)
    numeric_style = _with_font(_NUMERIC_CELL_STYLE, font)

    title_markup = escape(title[:MAX_TITLE_CHARS])
    if font is not None:
        title_markup = f'<font name="{font}">{title_markup}</font>'
    story: list[Any] = [_build_header_table(title_markup)]
    if subtitle:
        story.append(Paragraph(escape(subtitle[:500]), subtitle_style))
    meta = f"{escape(generated_label)}: {generated.strftime('%Y-%m-%d %H:%M:%S %Z').strip()}"
    meta += f" - {len(data_rows)}"
    story.append(Paragraph(meta, meta_style))
    if truncated_note:
        story.append(Paragraph(escape(truncated_note), meta_style))

    header = [Paragraph(escape(c.label), header_style) for c in cols]
    body = [
        [
            Paragraph(escape(text), numeric_style if c.align == "right" else cell_style)
            for c, text in zip(cols, texts, strict=True)
        ]
        for texts in cell_texts
    ]
    table = Table([header, *body], colWidths=_column_widths(cols, available_width), repeatRows=1)
    table.setStyle(_TABLE_STYLE)
    story.append(Spacer(1, 0.1 * cm))
    story.append(table)

    doc.build(story, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    return buffer.getvalue()
