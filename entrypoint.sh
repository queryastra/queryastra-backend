#!/bin/sh
set -e

# Aviso de segurança: senha admin ainda no valor padrão
if [ "${ADMIN_PASSWORD:-admin}" = "admin" ]; then
    echo "[entrypoint] AVISO: ADMIN_PASSWORD ainda é 'admin' (valor padrão). Defina ADMIN_PASSWORD via variável de ambiente antes de expor em produção." >&2
fi

# Aguarda o PostgreSQL estar pronto antes de aplicar migrations
echo "[entrypoint] Aguardando banco de dados..."
python - <<'PYEOF'
import asyncio, sys, os
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

async def wait_for_db():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        print("[entrypoint] Erro: DATABASE_URL não definida.", file=sys.stderr)
        sys.exit(1)
    engine = create_async_engine(url)
    for attempt in range(30):
        try:
            async with engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
            await engine.dispose()
            print(f"[entrypoint] Banco pronto (tentativa {attempt + 1}).")
            return
        except Exception as exc:
            print(f"[entrypoint] Banco não disponível (tentativa {attempt + 1}/30): {exc}")
            await asyncio.sleep(2)
    print("[entrypoint] Banco não ficou disponível em 60s. Abortando.", file=sys.stderr)
    sys.exit(1)

asyncio.run(wait_for_db())
PYEOF

echo "[entrypoint] Aplicando migrações Alembic..."
alembic upgrade head
echo "[entrypoint] Migrações aplicadas. Subindo servidor..."

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
