"""Testes do gerador generico de PDF de tabela (`app.services.table_pdf_service`)."""

from __future__ import annotations

import io
from datetime import UTC, datetime

import pytest
from reportlab.pdfbase import pdfmetrics

from app.services.table_pdf_service import (
    MAX_CELL_CHARS,
    MAX_ROWS,
    TableColumn,
    _cell_text,
    _column_widths,
    build_table_pdf,
    pick_font,
)

COLUMNS = [
    TableColumn(key="index", label="Index", weight=3),
    TableColumn(key="size", label="Size (MB)", align="right"),
    TableColumn(key="unused", label="Unused"),
]


def _page_count(pdf: bytes) -> int:
    return pdf.count(b"/Type /Page\n") + pdf.count(b"/Type /Page>>") + pdf.count(b"/Type /Page ")


def test_builds_valid_pdf_with_rows() -> None:
    pdf = build_table_pdf(
        title="Index health",
        subtitle="prod-db01 · sales · scanned 2026-11-17 10:00",
        columns=COLUMNS,
        rows=[{"index": "ix_orders_customer", "size": 12.5, "unused": True}],
        generated_at=datetime(2026, 11, 17, 12, 0, tzinfo=UTC),
    )
    assert pdf.startswith(b"%PDF-")
    assert pdf.rstrip().endswith(b"%%EOF")
    assert len(pdf) > 1000


def test_landscape_page_size() -> None:
    pdf = build_table_pdf(title="T", subtitle=None, columns=COLUMNS, rows=[])
    # A4 paisagem = 841.89 x 595.28 pt
    assert b"841.8898" in pdf
    assert b"595.2756" in pdf


def test_many_rows_span_multiple_pages() -> None:
    rows = [{"index": f"ix_{i}", "size": float(i), "unused": False} for i in range(400)]
    single = build_table_pdf(title="T", subtitle=None, columns=COLUMNS, rows=rows[:5])
    multi = build_table_pdf(title="T", subtitle=None, columns=COLUMNS, rows=rows)
    assert _page_count(multi) > _page_count(single)


def test_rows_are_capped() -> None:
    rows = [{"index": "x", "size": 1, "unused": False}] * (MAX_ROWS + 10)
    pdf = build_table_pdf(title="T", subtitle=None, columns=COLUMNS[:1], rows=rows)
    assert pdf.startswith(b"%PDF-")


def test_requires_columns() -> None:
    with pytest.raises(ValueError):
        build_table_pdf(title="T", subtitle=None, columns=[], rows=[])


def test_html_like_content_is_escaped() -> None:
    # reportlab Paragraph interpreta markup — sem escape, "<b" quebraria o parse.
    pdf = build_table_pdf(
        title="<script>",
        subtitle="a & b < c",
        columns=COLUMNS,
        rows=[{"index": "<b>not bold", "size": "1 < 2", "unused": None}],
    )
    assert pdf.startswith(b"%PDF-")


def test_cell_text_formatting() -> None:
    assert _cell_text(None) == ""
    assert _cell_text(True) == "Yes"
    assert _cell_text(1234.5) == "1,234.50"
    long = _cell_text("x" * (MAX_CELL_CHARS + 50))
    assert len(long) == MAX_CELL_CHARS
    assert long.endswith("…")


def test_column_widths_are_proportional() -> None:
    widths = _column_widths(COLUMNS, 500.0)
    assert sum(widths) == pytest.approx(500.0)
    assert widths[0] == pytest.approx(300.0)


@pytest.mark.parametrize(
    ("text", "font"),
    [
        ("Saúde de índices", None),
        ("インデックスの健全性", "HeiseiKakuGo-W5"),
        ("인덱스 상태", "HYGothic-Medium"),
        ("索引健康状况", "STSong-Light"),
    ],
)
def test_pick_font_by_script(text: str, font: str | None) -> None:
    assert pick_font([text]) == font
    if font:
        assert font in pdfmetrics.getRegisteredFontNames()


def test_cjk_title_renders() -> None:
    pdf = build_table_pdf(
        title="索引健康状况",
        subtitle="实例",
        columns=[TableColumn(key="a", label="名称")],
        rows=[{"a": "ix_1"}],
    )
    assert pdf.startswith(b"%PDF-")
    assert b"STSong-Light" in pdf
    assert io.BytesIO(pdf).getbuffer().nbytes > 0
