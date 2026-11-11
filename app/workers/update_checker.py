"""Background worker que verifica atualizacoes no GitHub Releases.

Roda a cada `check_interval_seconds` (default 24h). Armazena o resultado em
memoria (singleton `_current_status`); os endpoints em `app/api/v1/update.py`
consultam esse estado sem bater na API do GitHub a cada request.

Instalacao: quando rodando como binario PyInstaller (sys.frozen=True):
  - Windows: baixa novo .exe, escreve .bat que substitui o binario apos o processo
    sair e lanca o bat em processo separado, depois envia SIGTERM ao proprio processo.
  - POSIX: substitui o binario atomicamente com shutil.move e reinicia via os.execv.

Em modo desenvolvimento (nao-frozen) a instalacao e no-op — apenas loga.
"""

from __future__ import annotations

import asyncio
import os
import platform
import signal
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx

from app.core.logging import get_logger

logger = get_logger(__name__)

APP_VERSION = "0.1.3"
_GITHUB_RELEASES_URL = (
    "https://api.github.com/repos/queryastra/queryastra-backend/releases/latest"
)
_REQUEST_TIMEOUT = 15.0


@dataclass
class UpdateStatus:
    current_version: str
    latest_version: str | None
    update_available: bool
    download_url: str | None
    release_notes: str | None
    check_error: str | None


_current_status: UpdateStatus = UpdateStatus(
    current_version=APP_VERSION,
    latest_version=None,
    update_available=False,
    download_url=None,
    release_notes=None,
    check_error=None,
)


def get_update_status() -> UpdateStatus:
    return _current_status


class UpdateChecker:
    """Verifica periodicamente se ha nova versao disponivel no GitHub Releases."""

    def __init__(self, check_interval_seconds: int = 86400) -> None:
        self._interval = check_interval_seconds
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("UpdateChecker iniciado (check_interval=%ss)", self._interval)
        await self._check()
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
            except TimeoutError:
                await self._check()
        logger.info("UpdateChecker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _check(self) -> None:
        global _current_status  # noqa: PLW0603
        try:
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT, follow_redirects=True
            ) as client:
                resp = await client.get(
                    _GITHUB_RELEASES_URL,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                    },
                )
            if resp.status_code != 200:
                logger.warning("GitHub Releases retornou HTTP %s", resp.status_code)
                return

            data: dict = resp.json()
            latest_tag = data.get("tag_name", "").lstrip("v")
            assets: list[dict] = data.get("assets", [])
            release_notes: str = data.get("body", "") or ""
            download_url = _pick_asset(assets)
            update_available = bool(latest_tag) and latest_tag != APP_VERSION

            _current_status = UpdateStatus(
                current_version=APP_VERSION,
                latest_version=latest_tag or None,
                update_available=update_available,
                download_url=download_url,
                release_notes=release_notes[:4000],
                check_error=None,
            )
            if update_available:
                logger.info(
                    "Atualizacao disponivel: %s -> %s", APP_VERSION, latest_tag
                )
            else:
                logger.info("Versao atual (%s) esta atualizada", APP_VERSION)
        except Exception as exc:  # noqa: BLE001
            logger.warning("UpdateChecker: falha ao verificar atualizacao: %s", exc)
            _current_status = UpdateStatus(
                current_version=APP_VERSION,
                latest_version=None,
                update_available=False,
                download_url=None,
                release_notes=None,
                check_error=str(exc),
            )


def _pick_asset(assets: list[dict]) -> str | None:
    """Retorna a URL de download do asset correspondente ao OS atual."""
    os_key = {"Windows": "windows", "Linux": "linux", "Darwin": "macos"}.get(
        platform.system(), ""
    )
    for asset in assets:
        name: str = asset.get("name", "").lower()
        if os_key and os_key in name:
            return asset.get("browser_download_url")
    return None


async def perform_install(download_url: str) -> None:
    """Baixa o novo binario, substitui o atual e reinicia o processo.

    Quando nao rodando como binario PyInstaller (modo dev), apenas loga.
    """
    is_frozen = getattr(sys, "frozen", False)
    if not is_frozen:
        logger.info(
            "Modo desenvolvimento: atualizacao automatica nao suportada; "
            "atualize manualmente via git pull."
        )
        return

    current_exe = Path(sys.executable).resolve()
    tmp_path = current_exe.with_suffix(".new")

    logger.info("Baixando atualizacao de %s ...", download_url)
    async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
        resp = await client.get(download_url)
        resp.raise_for_status()
        tmp_path.write_bytes(resp.content)
    logger.info("Download concluido (%s bytes)", tmp_path.stat().st_size)

    if platform.system() == "Windows":
        _install_windows(current_exe, tmp_path)
    else:
        _install_posix(current_exe, tmp_path)


def _install_windows(current_exe: Path, tmp_path: Path) -> None:
    import subprocess

    bat = current_exe.with_suffix(".update.bat")
    bat.write_text(
        "@echo off\r\n"
        "timeout /t 3 /nobreak >nul\r\n"
        f'copy /y "{tmp_path}" "{current_exe}"\r\n'
        f'del "{tmp_path}"\r\n'
        f'start "" "{current_exe}"\r\n'
        'del "%~f0"\r\n',
        encoding="utf-8",
    )
    subprocess.Popen(  # noqa: S603
        ["cmd", "/c", str(bat)],
        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
        close_fds=True,
    )
    logger.info("Script de atualizacao agendado (%s). Encerrando processo...", bat)
    os.kill(os.getpid(), signal.CTRL_C_EVENT)


def _install_posix(current_exe: Path, tmp_path: Path) -> None:
    import shutil

    shutil.move(str(tmp_path), str(current_exe))
    current_exe.chmod(0o755)
    logger.info("Binario substituido. Reiniciando com os.execv...")
    os.execv(str(current_exe), [str(current_exe)])
