"""Exportacao generica de tabela em PDF — `POST /exports/table-pdf`.

O frontend envia o que esta filtrado/visivel (colunas + linhas ja formatadas) e recebe o PDF
pronto (A4 paisagem, cabecalho QueryAstra). Nenhum dado e lido do banco aqui — a autorizacao de
ver os dados ja aconteceu no endpoint que os entregou a tela; exigimos apenas autenticacao.
Tamanhos validados no schema (colunas, linhas, tamanho de celula) para evitar abuso.
"""

from __future__ import annotations

import asyncio
import re
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, Field, field_validator

from app.api.deps import CurrentUser, get_current_user
from app.services.table_pdf_service import (
    MAX_CELL_CHARS,
    MAX_COLUMNS,
    MAX_ROWS,
    MAX_TITLE_CHARS,
    TableColumn,
    build_table_pdf,
)

router = APIRouter()

CellValue = str | int | float | bool | None


class TablePdfColumn(BaseModel):
    key: str = Field(min_length=1, max_length=100)
    label: str = Field(max_length=200)
    align: Literal["left", "right"] = "left"
    weight: float = Field(default=1.0, gt=0, le=20)


class TablePdfRequest(BaseModel):
    title: str = Field(min_length=1, max_length=MAX_TITLE_CHARS)
    subtitle: str | None = Field(default=None, max_length=500)
    filename: str | None = Field(default=None, max_length=120)
    generated_label: str = Field(default="Generated at", max_length=60)
    truncated_note: str | None = Field(default=None, max_length=300)
    columns: list[TablePdfColumn] = Field(min_length=1, max_length=MAX_COLUMNS)
    rows: list[dict[str, CellValue]] = Field(max_length=MAX_ROWS)

    @field_validator("rows")
    @classmethod
    def _check_cells(cls, rows: list[dict[str, CellValue]]) -> list[dict[str, CellValue]]:
        for row in rows:
            if len(row) > MAX_COLUMNS * 2:
                raise ValueError("too many cells in row")
            for value in row.values():
                if isinstance(value, str) and len(value) > MAX_CELL_CHARS:
                    raise ValueError(f"cell exceeds {MAX_CELL_CHARS} characters")
        return rows


_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(name: str | None) -> str:
    base = _SAFE_FILENAME.sub("_", (name or "export").strip()).strip("._") or "export"
    base = base[:100]
    return base if base.lower().endswith(".pdf") else f"{base}.pdf"


@router.post(
    "/table-pdf",
    response_class=Response,
    responses={200: {"content": {"application/pdf": {}}}},
)
async def export_table_pdf(
    body: TablePdfRequest,
    _user: Annotated[CurrentUser, Depends(get_current_user)],
) -> Response:
    rows: list[dict[str, Any]] = [dict(r) for r in body.rows]
    # reportlab e CPU-bound e sincrono: roda fora do event loop.
    pdf = await asyncio.to_thread(
        build_table_pdf,
        title=body.title,
        subtitle=body.subtitle,
        columns=[
            TableColumn(key=c.key, label=c.label, align=c.align, weight=c.weight)
            for c in body.columns
        ],
        rows=rows,
        generated_label=body.generated_label,
        truncated_note=body.truncated_note,
    )
    filename = safe_filename(body.filename)
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
