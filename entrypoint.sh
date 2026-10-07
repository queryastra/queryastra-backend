#!/bin/sh
set -e

echo "[entrypoint] Aplicando migrações Alembic..."
alembic upgrade head
echo "[entrypoint] Migrações aplicadas. Subindo servidor..."

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
