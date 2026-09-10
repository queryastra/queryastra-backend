# ab-xql-pm-backend

Backend FastAPI do **AB-XQL-PM** — observabilidade agentless de PostgreSQL e SQL Server.

Este repositorio contem apenas o **codigo**. As decisoes de arquitetura, contratos de API/coleta,
schema de dados e regras de negocio vivem no repositorio separado `ab-xql-pm-documentos` (fonte de
verdade externa) — consulte sempre antes de alterar comportamento:

- `ARCHITECTURE_SPEC.md`
- `Docs/specs/01-collection-contract.md`, `Docs/specs/02-backend-contract.md`
- `Docs/database-schema.md`, `Docs/collector-queries.md`, `Docs/wait-taxonomy.md`
- `Docs/rules/00-source-of-truth.md` a `04-engine-capabilities.md`

## Status

Fase 1 (MVP) em construcao. Este e o **scaffold fundacional**: estrutura de camadas
(`api`/`services`/`collectors`/`normalizers`/`repositories`/`workers`), modelos SQLAlchemy das
tabelas relacionais simples, migration Alembic baseline, e stubs corretos (sem logica de negocio
real) para os endpoints e adaptadores de coleta. Ver `Docs/product-vision-and-roadmap.md` no
repositorio de documentos para o roadmap completo.

## Requisitos

- Python 3.12 (fixado em `.python-version`; **nao** usar 3.14, `pyodbc`/`pymssql` podem nao ter
  wheel disponivel).
- [`uv`](https://docs.astral.sh/uv/) para gerenciar o ambiente e as dependencias.
- PostgreSQL/TimescaleDB acessivel via `DATABASE_URL` (ver `.env.example`) para rodar migrations e
  a aplicacao de verdade — os testes automatizados nao dependem de banco real.

O backend espera que o repositorio `ab-xql-pm-documentos` esteja disponivel
localmente para consulta. Os links para `ARCHITECTURE_SPEC.md` e `Docs/` neste
README sao referencias ao repositorio de documentacao, nao arquivos deste repo.

## Setup local

```bash
uv python pin 3.12
uv sync
cp .env.example .env   # ajustar DATABASE_URL e demais variaveis
uv run alembic upgrade head   # requer Postgres/TimescaleDB acessivel
uv run uvicorn app.main:app --reload
```

Com a aplicacao em execucao, a API fica em `http://localhost:8000` e a
documentacao OpenAPI em `http://localhost:8000/docs`.

## Qualidade

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy app
uv run pytest
```

## Camadas (`Docs/specs/02-backend-contract.md`)

- `app/api`: contratos HTTP e WebSocket (autenticacao fica para fase posterior).
- `app/services`: casos de uso e orquestracao.
- `app/collectors`: conexoes e queries por engine (PostgreSQL/SQL Server), sempre atras da
  interface `Collector` (`app/collectors/base.py`).
- `app/normalizers`: contrato cross-engine (taxonomia de wait, modelos normalizados).
- `app/repositories`: acesso a TimescaleDB.
- `app/workers`: polling, agregacao e alertas (loops `asyncio`, fora das rotas HTTP).
- `app/security`, `app/masking`: referencia segura a credencial e mascaramento obrigatorio de SQL
  (`Docs/rules/01-credential-security.md`, `Docs/rules/03-sql-privacy.md`).

## Regras nao-negociaveis

- Nunca gravar senha em texto puro em tabela, log ou payload — apenas referencia a segredo
  (`app/security/credential_reference.py`).
- Mascarar literais de SQL antes de persistir ou exibir (`app/masking/sql_masking.py`).
- Toda coleta e somente leitura, com timeout, limite de linhas e respeito a janela de manutencao.
- Erro de capacidade (permissao/versao) degrada so a capacidade afetada, nunca derruba a coleta
  inteira (`Docs/rules/04-engine-capabilities.md`).
