@echo off
setlocal enabledelayedexpansion
REM QueryAstra — script de configuracao inicial (Windows 10/11/Server 2019/2022)
REM Requer: Docker Desktop instalado e rodando.
REM Uso: duplo clique ou executar em cmd/PowerShell como administrador.

echo.
echo   =============================================
echo     QueryAstra - Setup
echo   =============================================
echo.

REM ── Verificar Docker ─────────────────────────────────────────────────────────
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Docker nao encontrado ou nao esta rodando.
    echo        Instale o Docker Desktop em: https://docs.docker.com/desktop/windows/
    pause & exit /b 1
)
echo [OK] Docker encontrado.

REM ── Verificar docker compose ──────────────────────────────────────────────────
docker compose version >nul 2>&1
if errorlevel 1 (
    docker-compose version >nul 2>&1
    if errorlevel 1 (
        echo [ERRO] Docker Compose nao encontrado.
        pause & exit /b 1
    )
    set "COMPOSE=docker-compose"
) else (
    set "COMPOSE=docker compose"
)
echo [OK] Docker Compose encontrado.

REM ── Localizar raiz do projeto ─────────────────────────────────────────────────
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%..\..\"
pushd "%ROOT%"
set "ROOT=%CD%"
popd

REM ── Verificar .env ────────────────────────────────────────────────────────────
if exist "%ROOT%\.env" (
    echo [AVISO] .env ja existe em %ROOT%\.env
    echo         Delete o arquivo e rode novamente para regenerar segredos.
    goto :build
)

REM ── Gerar chaves via container Python ────────────────────────────────────────
echo.
echo Gerando chaves de seguranca...

for /f "delims=" %%K in ('docker run --rm python:3.12-slim python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"') do set "CRED_KEY=%%K"
for /f "delims=" %%J in ('docker run --rm python:3.12-slim python -c "import secrets; print(secrets.token_hex(32))"') do set "JWT_KEY=%%J"

if "!CRED_KEY!"=="" (
    echo [ERRO] Falha ao gerar CREDENTIAL_ENCRYPTION_KEY.
    pause & exit /b 1
)
echo [OK] Chaves geradas.

REM ── Coletar configuracoes do usuario ─────────────────────────────────────────
echo.
set "ADMIN_PASS=admin"
set /p "ADMIN_PASS=Senha do administrador [Enter para 'admin']: "
if "!ADMIN_PASS!"=="" set "ADMIN_PASS=admin"

set "DB_PASS=queryastra"
set /p "DB_PASS=Senha do banco de dados [Enter para 'queryastra']: "
if "!DB_PASS!"=="" set "DB_PASS=queryastra"

set "API_URL=http://localhost:8000"
set /p "API_URL=URL publica do backend [Enter para http://localhost:8000]: "
if "!API_URL!"=="" set "API_URL=http://localhost:8000"

set "WS_URL=!API_URL:http://=ws://!"
set "WS_URL=!WS_URL:https://=wss://!"

REM ── Criar .env ────────────────────────────────────────────────────────────────
(
    echo DB_PASSWORD=!DB_PASS!
    echo CREDENTIAL_ENCRYPTION_KEY=!CRED_KEY!
    echo JWT_SECRET_KEY=!JWT_KEY!
    echo ADMIN_USERNAME=admin
    echo ADMIN_PASSWORD=!ADMIN_PASS!
    echo NEXT_PUBLIC_API_BASE_URL=!API_URL!
    echo NEXT_PUBLIC_WS_URL=!WS_URL!
    echo FRONTEND_URL=http://localhost:3000
    echo ENVIRONMENT=production
    echo LOG_LEVEL=INFO
) > "%ROOT%\.env"
echo [OK] .env criado em %ROOT%\.env

:build
REM ── Build e start ─────────────────────────────────────────────────────────────
echo.
echo Construindo imagens Docker (pode levar alguns minutos na primeira vez)...
%COMPOSE% -f "%ROOT%\docker-compose.yml" --env-file "%ROOT%\.env" build
if errorlevel 1 (
    echo [ERRO] Falha no build. Verifique o Docker Desktop.
    pause & exit /b 1
)

echo.
echo Iniciando servicos...
%COMPOSE% -f "%ROOT%\docker-compose.yml" --env-file "%ROOT%\.env" up -d
if errorlevel 1 (
    echo [ERRO] Falha ao iniciar os containers.
    pause & exit /b 1
)

REM ── Aguardar backend ──────────────────────────────────────────────────────────
echo.
echo Aguardando o backend ficar pronto (pode levar ate 90 segundos)...
set /a TRIES=0
:wait_loop
set /a TRIES+=1
curl -sf http://localhost:8000/health >nul 2>&1
if not errorlevel 1 goto :ready
if !TRIES! geq 30 (
    echo [AVISO] Backend ainda nao respondeu. Verifique: %COMPOSE% logs backend
    goto :done
)
timeout /t 3 /nobreak >nul
goto :wait_loop

:ready
echo [OK] Backend respondendo.

:done
echo.
echo   ============================================================
echo     QueryAstra esta no ar!
echo.
echo     Frontend:  http://localhost:3000
echo     Backend:   http://localhost:8000
echo     API Docs:  http://localhost:8000/docs
echo   ============================================================
echo.
pause
endlocal
