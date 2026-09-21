"""Testes de `app.masking.pii_classification.is_pii_column`."""

from __future__ import annotations

import pytest

from app.masking.pii_classification import is_pii_column


@pytest.mark.parametrize(
    "column",
    [
        "cpf",
        "CPF",
        "cnpj",
        "rg",
        "nome",
        "sobrenome",
        "email",
        "e-mail",
        "telefone",
        "celular",
        "endereco",
        "endereço",
        "logradouro",
        "bairro",
        "cep",
        "senha",
        "password",
        "cartao",
        "cartão",
        "cvv",
        "conta",
        "iban",
        "nascimento",
        "matricula",
        "matrícula",
        "renda",
        "salario",
        "salário",
        "titular",
        "c.cpf",
        "cliente_nome",
        "[dbo].[Clientes].[Email]",
    ],
)
def test_recognizes_pii_column(column: str) -> None:
    assert is_pii_column(column) is True


@pytest.mark.parametrize(
    "column",
    ["status", "id", "retries", "created_at", "database_name", "quantidade", "valor_total"],
)
def test_does_not_flag_non_pii_column(column: str) -> None:
    assert is_pii_column(column) is False
