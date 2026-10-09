#Requires -Version 5.1
<#
.SYNOPSIS
    QueryAstra — setup inicial via Docker Compose (Windows PowerShell)
.DESCRIPTION
    Verifica Docker, gera chaves criptográficas, cria .env e sobe os containers.
    Requer Docker Desktop instalado e em execução.
.EXAMPLE
    .\setup.ps1
#>

$ErrorActionPreference = 'Stop'

function Write-Step($msg) { Write-Host "[....] $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "[ERRO] $msg" -ForegroundColor Red; Read-Host "Pressione Enter para sair"; exit 1 }

Write-Host ""
Write-Host "  =============================================" -ForegroundColor Magenta
Write-Host "    QueryAstra - Setup (PowerShell)"            -ForegroundColor Magenta
Write-Host "  =============================================" -ForegroundColor Magenta
Write-Host ""

# ── Verificar Docker ──────────────────────────────────────────────────────────
Write-Step "Verificando Docker..."
try { docker info 2>&1 | Out-Null; if ($LASTEXITCODE -ne 0) { throw } }
catch { Write-Err "Docker não encontrado ou não está rodando. Instale: https://docs.docker.com/desktop/install/windows-install/" }
Write-Ok "Docker encontrado."

$compose = if ((docker compose version 2>&1) -and $LASTEXITCODE -eq 0) { "docker compose" } else { "docker-compose" }
Write-Ok "Docker Compose encontrado ($compose)."

# ── Localizar raiz ────────────────────────────────────────────────────────────
$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..\")).Path

# ── Verificar .env ────────────────────────────────────────────────────────────
$envFile = Join-Path $root ".env"
if (Test-Path $envFile) {
    Write-Warn ".env já existe em $envFile — pulando geração de segredos."
    Write-Warn "Delete o arquivo e rode novamente para regenerar."
} else {
    # ── Gerar chaves via container Python ─────────────────────────────────────
    Write-Step "Gerando chaves de segurança (usando container Python)..."
    $credKey = (docker run --rm python:3.12-slim python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())").Trim()
    $jwtKey  = (docker run --rm python:3.12-slim python -c "import secrets; print(secrets.token_hex(32))").Trim()
    if (-not $credKey) { Write-Err "Falha ao gerar CREDENTIAL_ENCRYPTION_KEY." }
    Write-Ok "Chaves geradas."

    # ── Coletar configurações ─────────────────────────────────────────────────
    Write-Host ""
    $adminPass = Read-Host "Senha do administrador [Enter para 'admin']"
    if (-not $adminPass) { $adminPass = "admin" }

    $dbPass = Read-Host "Senha do banco de dados [Enter para 'queryastra']"
    if (-not $dbPass) { $dbPass = "queryastra" }

    $apiUrl = Read-Host "URL pública do backend [Enter para http://localhost:8000]"
    if (-not $apiUrl) { $apiUrl = "http://localhost:8000" }

    $wsUrl = $apiUrl -replace '^http://', 'ws://' -replace '^https://', 'wss://'

    # ── Criar .env ────────────────────────────────────────────────────────────
    @"
DB_PASSWORD=$dbPass
CREDENTIAL_ENCRYPTION_KEY=$credKey
JWT_SECRET_KEY=$jwtKey
ADMIN_USERNAME=admin
ADMIN_PASSWORD=$adminPass
NEXT_PUBLIC_API_BASE_URL=$apiUrl
NEXT_PUBLIC_WS_URL=$wsUrl
FRONTEND_URL=http://localhost:3000
ENVIRONMENT=production
LOG_LEVEL=INFO
"@ | Set-Content -Encoding utf8 -Path $envFile
    Write-Ok ".env criado em $envFile"
}

# ── Build ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Step "Construindo imagens Docker (pode levar alguns minutos)..."
& cmd /c "$compose -f `"$root\docker-compose.yml`" --env-file `"$envFile`" build"
if ($LASTEXITCODE -ne 0) { Write-Err "Falha no build. Verifique o Docker Desktop." }

Write-Host ""
Write-Step "Iniciando serviços..."
& cmd /c "$compose -f `"$root\docker-compose.yml`" --env-file `"$envFile`" up -d"
if ($LASTEXITCODE -ne 0) { Write-Err "Falha ao iniciar os containers." }

# ── Aguardar backend ──────────────────────────────────────────────────────────
Write-Host ""
Write-Step "Aguardando backend (até 90 segundos)..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($r.StatusCode -eq 200) { $ready = $true; break }
    } catch {}
    Start-Sleep -Seconds 3
    Write-Host -NoNewline "."
}
Write-Host ""
if ($ready) { Write-Ok "Backend respondendo." }
else { Write-Warn "Backend ainda não respondeu. Verifique: $compose logs backend" }

Write-Host ""
Write-Host "  ============================================================" -ForegroundColor Magenta
Write-Host "    QueryAstra está no ar!                                    " -ForegroundColor Magenta
Write-Host "                                                              " -ForegroundColor Magenta
Write-Host "    Frontend:  http://localhost:3000                          " -ForegroundColor Magenta
Write-Host "    Backend:   http://localhost:8000                          " -ForegroundColor Magenta
Write-Host "    API Docs:  http://localhost:8000/docs                     " -ForegroundColor Magenta
Write-Host "  ============================================================" -ForegroundColor Magenta
Write-Host ""
Read-Host "Pressione Enter para sair"
