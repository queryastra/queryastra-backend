"""Worker de relatorios agendados â€" executa relatorios cujo `next_run_at` venceu.

Padrao identico aos demais workers deste projeto (MissingIndexSupervisor, BackupStatusSupervisor,
etc.): construtor recebe `(async_session_factory, check_interval_seconds)`, expoe `.run()` e
`.stop()`. Desligado por padrao â€" habilitar via `REPORT_WORKER_ENABLED=true` no `.env`.

Para cada relatorio vencido:
  1. Gera o PDF em memoria.
  2. Se houver email_recipients E e-mail configurado (SMTP ou Brevo), envia o PDF anexado.
  3. Atualiza `last_run_at`, `last_run_status`, `next_run_at` no banco.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from croniter import croniter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.models.scheduled_report import ScheduledReport
from app.repositories.instances_repository import (
    InstancesRepository,  # noqa: F401 (used inside method)
)
from app.repositories.report_execution_logs_repository import ReportExecutionLogsRepository
from app.repositories.scheduled_reports_repository import ScheduledReportsRepository
from app.services.email_service import (
    EmailAttachment,
    OutgoingEmail,
    resolve_email_config,
    send_email,
)
from app.services.report_service import generate_report

logger = get_logger(__name__)


def _compute_next_run(cron_expr: str, after: datetime) -> datetime | None:
    try:
        cron = croniter(cron_expr, after)
        return cron.get_next(datetime).replace(tzinfo=UTC)
    except Exception:
        logger.warning("Expressao cron invalida: %r", cron_expr)
        return None


class ReportWorker:
    def __init__(
        self, async_session_factory: async_sessionmaker[AsyncSession], check_interval_seconds: int
    ) -> None:
        self._factory = async_session_factory
        self._interval = check_interval_seconds
        self._running = False

    def stop(self) -> None:
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("ReportWorker iniciado (check_interval=%ss)", self._interval)
        while self._running:
            try:
                await self._check_due_reports()
            except Exception:
                logger.exception("Erro no ciclo do ReportWorker")
            await asyncio.sleep(self._interval)
        logger.info("ReportWorker encerrado")

    async def _check_due_reports(self) -> None:
        now = datetime.now(UTC)
        async with self._factory() as session:
            repo = ScheduledReportsRepository(session)
            due = await repo.list_due(now)
            if not due:
                return
            logger.info("ReportWorker: %d relatorio(s) a executar", len(due))
            for report in due:
                await self._run_one(session, report, now)
            await session.commit()

    async def _run_one(self, session: AsyncSession, report: ScheduledReport, now: datetime) -> None:
        instances_repo = InstancesRepository(session)
        instance = await instances_repo.get_by_id(report.instance_id)
        if instance is None:
            logger.warning(
                "Instancia %s nao encontrada para relatorio %s", report.instance_id, report.id
            )
            return

        try:
            pdf_bytes = await generate_report(
                session=session,
                report_type=report.report_type,
                instance=instance,
                title=report.title,
                params=report.params,
            )
            status = "ok"
            error: str | None = None
        except Exception as exc:
            logger.exception("Erro ao gerar relatorio %s", report.id)
            status = "error"
            error = str(exc)
            next_run = _compute_next_run(report.schedule_cron, now)
            repo = ScheduledReportsRepository(session)
            await repo.update_after_run(report, status, error, next_run)
            log_repo = ReportExecutionLogsRepository(session)
            await log_repo.insert(
                report_id=report.id,
                report_title=report.title,
                report_type=report.report_type,
                instance_id=instance.id,
                instance_name=instance.name,
                executed_at=now,
                status=status,
                error=error,
                email_recipients=report.email_recipients or [],
            )
            return

        # Tenta enviar email se configurado; falha de envio não cancela a execução mas é registrada.
        if report.email_recipients:
            email_error = await self._try_send_email(session, report, pdf_bytes)
            if email_error:
                error = f"PDF gerado; falha ao enviar email: {email_error}"

        next_run = _compute_next_run(report.schedule_cron, now) if report.is_recurring else None
        repo = ScheduledReportsRepository(session)
        await repo.update_after_run(report, status, error, next_run)

        log_repo = ReportExecutionLogsRepository(session)
        await log_repo.insert(
            report_id=report.id,
            report_title=report.title,
            report_type=report.report_type,
            instance_id=instance.id,
            instance_name=instance.name,
            executed_at=now,
            status=status,
            error=error,
            email_recipients=report.email_recipients or [],
        )
        logger.info("Relatorio %s gerado com sucesso (proxima execucao: %s)", report.id, next_run)

    async def _try_send_email(
        self, session: AsyncSession, report: ScheduledReport, pdf_bytes: bytes
    ) -> str | None:
        """Envia o PDF por email pelo provedor configurado (`app.services.email_service`).
        Retorna a mensagem de erro em caso de falha, None se ok ou se e-mail nao configurado."""
        config = await resolve_email_config(session)
        if not config.is_configured:
            logger.debug("E-mail nao configurado - relatorio %s nao enviado", report.id)
            return None

        filename = f"relatorio-{report.report_type}-{datetime.now(UTC).strftime('%Y%m%d')}.pdf"
        email = OutgoingEmail(
            recipients=list(report.email_recipients),
            subject=f"[QueryAstra] {report.title}",
            text_body=(
                f"Relatório gerado automaticamente: {report.title}\nInstância: {report.instance_id}"
            ),
            attachments=[EmailAttachment(filename, pdf_bytes, "application/pdf")],
        )
        try:
            await send_email(config, email)
            logger.info("Email enviado para relatorio %s -> %s", report.id, report.email_recipients)
            return None
        except Exception as exc:
            logger.exception(
                "Falha ao enviar email para relatorio %s (nao bloqueia execucao)", report.id
            )
            return str(exc)
