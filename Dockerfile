FROM python:3.12-slim

# Driver ODBC do SO necessario para pyodbc (coletor SQL Server) — ver Docs/collector-queries.md.
# Se a imagem final nao precisar do coletor SQL Server, este bloco pode ser removido.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
# ibm-db não tem wheel para linux/aarch64 — tenta instalar tudo; se falhar (ARM),
# reinstala sem ibm-db. O adaptador Db2 detecta a ausência em runtime e reporta erro
# claro, sem derrubar o restante da aplicação.
RUN pip install --no-cache-dir uv \
    && (uv sync --frozen --no-dev \
        || (echo "Retrying without ibm-db (ARM/unsupported platform)..." \
            && uv sync --frozen --no-dev --no-install-package ibm-db))

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

RUN chmod +x entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
