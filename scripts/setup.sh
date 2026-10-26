#!/usr/bin/env bash
# QueryAstra — script de configuração inicial (Linux / macOS)
# Uso: bash setup.sh
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[AVISO]${NC} $*"; }
err()  { echo -e "${RED}[ERRO]${NC} $*" >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo ""
echo "  ╔═══════════════════════════════════╗"
echo "  ║       QueryAstra — Setup          ║"
echo "  ╚═══════════════════════════════════╝"
echo ""

# ── Verificar dependências ────────────────────────────────────────────────────
command -v docker  >/dev/null 2>&1 || err "Docker não encontrado. Instale em https://docs.docker.com/get-docker/"
command -v python3 >/dev/null 2>&1 || err "python3 não encontrado. Instale via apt/dnf/brew."
ok "Docker e python3 encontrados."

COMPOSE_CMD=""
if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  err "Docker Compose não encontrado. Instale em https://docs.docker.com/compose/install/"
fi
ok "Docker Compose encontrado ($COMPOSE_CMD)."

# ── Verificar .env ────────────────────────────────────────────────────────────
ENV_FILE="$ROOT/.env"
if [[ -f "$ENV_FILE" ]]; then
  warn ".env já existe em $ENV_FILE — pulando geração de segredos."
  warn "Delete o arquivo e rode novamente para regenerar."
else
  echo ""
  echo "Gerando chaves de segurança..."

  CRED_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null \
    || err "Falha ao gerar CREDENTIAL_ENCRYPTION_KEY. Certifique-se de que 'cryptography' está instalado: pip3 install cryptography")
  JWT_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")

  echo ""
  echo "Usuário administrador padrão: admin"
  echo -n "Senha do administrador [Enter para 'admin']: "
  read -r ADMIN_PASS
  ADMIN_PASS="${ADMIN_PASS:-admin}"

  echo -n "Senha do banco de dados [Enter para 'queryastra']: "
  read -r DB_PASS
  DB_PASS="${DB_PASS:-queryastra}"

  echo -n "URL pública do backend (ex: http://meu-servidor:8000) [Enter para http://localhost:8000]: "
  read -r API_URL
  API_URL="${API_URL:-http://localhost:8000}"

  WS_URL="${API_URL/http:/ws:}"
  WS_URL="${WS_URL/https:/wss:}"

  cat > "$ENV_FILE" <<EOF
DB_PASSWORD=$DB_PASS
CREDENTIAL_ENCRYPTION_KEY=$CRED_KEY
JWT_SECRET_KEY=$JWT_KEY
ADMIN_USERNAME=admin
ADMIN_PASSWORD=$ADMIN_PASS
NEXT_PUBLIC_API_BASE_URL=$API_URL
NEXT_PUBLIC_WS_URL=$WS_URL
FRONTEND_URL=http://localhost:3001
ENVIRONMENT=production
LOG_LEVEL=INFO
EOF

  ok ".env criado em $ENV_FILE"
fi

# ── Construir e subir containers ──────────────────────────────────────────────
echo ""
echo "Construindo imagens Docker (pode levar alguns minutos na primeira vez)..."
$COMPOSE_CMD -f "$ROOT/docker-compose.yml" --env-file "$ENV_FILE" build

echo ""
echo "Iniciando serviços..."
$COMPOSE_CMD -f "$ROOT/docker-compose.yml" --env-file "$ENV_FILE" up -d

# ── Aguardar backend ──────────────────────────────────────────────────────────
echo ""
echo -n "Aguardando o backend ficar pronto"
for i in $(seq 1 30); do
  if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    echo ""
    ok "Backend respondendo."
    break
  fi
  echo -n "."
  sleep 3
  if [[ $i -eq 30 ]]; then
    echo ""
    warn "Backend ainda não respondeu após 90s. Verifique os logs: $COMPOSE_CMD logs backend"
  fi
done

echo ""
echo "  ╔══════════════════════════════════════════════════════════╗"
echo "  ║  QueryAstra está no ar!                                  ║"
echo "  ║                                                          ║"
echo "  ║  Frontend:  http://localhost:3001                        ║"
echo "  ║  Backend:   http://localhost:8000                        ║"
echo "  ║  API Docs:  http://localhost:8000/docs                   ║"
echo "  ╚══════════════════════════════════════════════════════════╝"
echo ""
