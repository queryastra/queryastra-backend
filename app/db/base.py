"""Declarative base do SQLAlchemy.

Todos os modelos ORM (`app/models`) herdam de `Base`. `migrations/env.py` importa `Base.metadata`
para o autogenerate do Alembic — qualquer modelo novo precisa ser importado em
`app/models/__init__.py` para ser enxergado pela migration.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base declarativa comum a todos os modelos ORM da aplicacao."""
