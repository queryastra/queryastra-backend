"""Fixtures de integracao de API — override de autenticacao para testes existentes.

Injeta `get_current_user` com um stub que retorna um `CurrentUser` admin em TODOS os testes de
integracao de API. Isso evita que os endpoints protegidos devolvam 401 nos testes existentes,
sem exigir que cada teste passe o header Authorization manualmente.

Os testes de `test_auth_endpoints.py` constroem seus proprios tokens JWT reais e NAO dependem
deste override — eles testam o comportamento real de autenticacao/autorizacao.
"""

from __future__ import annotations

import pytest

from app.api.deps import CurrentUser, get_current_user
from app.main import app


def _stub_admin() -> CurrentUser:
    """Stub de CurrentUser com papel admin — satisfaz qualquer dependencia de auth."""
    return CurrentUser(user_id=1, username="test", role="admin")


@pytest.fixture(autouse=True)
def override_auth() -> None:
    """Override automatico de `get_current_user` para todos os testes neste pacote."""
    app.dependency_overrides[get_current_user] = _stub_admin
    yield
    app.dependency_overrides.pop(get_current_user, None)
