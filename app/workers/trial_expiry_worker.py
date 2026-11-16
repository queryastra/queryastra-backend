"""Worker de notificacao de expiracao de trial.

Verifica periodicamente o plano atual e envia e-mail de aviso quando o trial
esta a 7 dias ou 1 dia de expirar. Cada marco e notificado apenas uma vez por
sessao do processo (tracking em memoria — suficiente para nao spammar o admin;
uma notificacao extra num restart eventual e aceitavel dada a simplicidade).
"""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.logging import get_logger
from app.repositories.system_license_repository import SystemLicenseRepository
from app.services.plan_service import PlanService

logger = get_logger(__name__)

_NOTIFY_AT_DAYS: frozenset[int] = frozenset({7, 1})
_WELCOME_SENTINEL = 999  # marks welcome email already sent in this session


class TrialExpiryWorker:
    """Envia e-mail de aviso (7 dias e 1 dia antes) quando o plano e trial."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        interval_seconds: int = 3600,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._interval_seconds = interval_seconds
        self._stop_event = asyncio.Event()
        self._notified: set[int] = set()

    async def run(self) -> None:
        logger.info("TrialExpiryWorker iniciado (interval=%ss)", self._interval_seconds)
        while not self._stop_event.is_set():
            try:
                await self._run_cycle()
            except Exception:  # noqa: BLE001 - ciclo nunca derruba o worker
                logger.exception("Erro inesperado em TrialExpiryWorker._run_cycle")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass
        logger.info("TrialExpiryWorker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_cycle(self) -> None:
        async with self._session_factory() as session:
            license_row = await SystemLicenseRepository(session).get()

        if license_row is None or license_row.plan_id != "trial":
            return

        plan_svc = PlanService(
            trial_started_at=license_row.trial_started_at,
            plan_id=license_row.plan_id,
            subscription_expires_at=license_row.subscription_expires_at,
        )
        days_remaining = plan_svc.trial_days_remaining()
        if days_remaining is None:
            return

        if days_remaining >= 29 and _WELCOME_SENTINEL not in self._notified:
            await self._send_welcome()
            self._notified.add(_WELCOME_SENTINEL)

        for milestone in _NOTIFY_AT_DAYS:
            if days_remaining <= milestone and milestone not in self._notified:
                await self._send_notice(days_remaining, milestone)
                self._notified.add(milestone)

    async def _send_welcome(self) -> None:
        s = self._settings
        if not s.alert_smtp_host or not s.alert_email_to:
            logger.debug("TrialExpiryWorker: SMTP não configurado — welcome email ignorado")
            return

        recipients = [a.strip() for a in s.alert_email_to.split(",") if a.strip()]
        if not recipients:
            return

        msg = EmailMessage()
        msg["Subject"] = "Welcome to QueryAstra — your 30-day trial has started"
        msg["From"] = s.alert_smtp_from or "noreply@queryastra.local"
        msg["To"] = ", ".join(recipients)
        msg.set_content(
            "Welcome to QueryAstra! Your 30-day trial has started. "
            "Open the app at http://localhost:8124 to get started.",
            subtype="plain",
        )
        msg.add_alternative(_build_welcome_html(), subtype="html")

        try:
            await asyncio.to_thread(_smtp_send, msg, s)
            logger.info("TrialExpiryWorker: welcome email enviado")
        except Exception:  # noqa: BLE001
            logger.exception("TrialExpiryWorker: falha ao enviar welcome email")

    async def _send_notice(self, days_remaining: int, milestone: int) -> None:
        s = self._settings
        if not s.alert_smtp_host or not s.alert_email_to:
            logger.warning(
                "TrialExpiryWorker: SMTP nao configurado — aviso de trial nao enviado "
                "(days_remaining=%s)",
                days_remaining,
            )
            return

        recipients = [a.strip() for a in s.alert_email_to.split(",") if a.strip()]
        if not recipients:
            return

        if days_remaining == 0:
            subject = "QueryAstra Trial Expires Today"
        elif days_remaining == 1:
            subject = "QueryAstra Trial Expires Tomorrow"
        else:
            subject = f"QueryAstra Trial Expiring in {days_remaining} Days"

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = s.alert_smtp_from or "noreply@queryastra.local"
        msg["To"] = ", ".join(recipients)
        msg.set_content(
            f"Your QueryAstra trial expires in {days_remaining} day(s). "
            "Upgrade at https://queryastra.com/pricing to keep access.",
            subtype="plain",
        )
        msg.add_alternative(_build_html(days_remaining), subtype="html")

        try:
            await asyncio.to_thread(_smtp_send, msg, s)
            logger.info(
                "TrialExpiryWorker: aviso enviado (days_remaining=%s, milestone=%s)",
                days_remaining,
                milestone,
            )
        except Exception:  # noqa: BLE001 - falha de envio nunca derruba o worker
            logger.exception(
                "TrialExpiryWorker: falha ao enviar aviso (days_remaining=%s)", days_remaining
            )


def _smtp_send(msg: EmailMessage, s: Settings) -> None:
    with smtplib.SMTP(s.alert_smtp_host, s.alert_smtp_port, timeout=10.0) as smtp:  # type: ignore[arg-type]
        smtp.starttls()
        if s.alert_smtp_username and s.alert_smtp_password:
            smtp.login(s.alert_smtp_username, s.alert_smtp_password)
        smtp.send_message(msg)


def _build_welcome_html() -> str:
    return """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Welcome to QueryAstra</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:'Helvetica Neue',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:32px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#35063e 0%,#7b2d8b 60%,#541060 100%);padding:32px 40px;text-align:center;">
          <div style="font-size:2.5rem;margin-bottom:8px;">&#x1FA90;</div>
          <div style="font-size:1.5rem;font-weight:700;color:#ffffff;letter-spacing:-0.02em;">QueryAstra</div>
          <div style="font-size:0.75rem;color:rgba(255,255,255,0.6);letter-spacing:0.06em;text-transform:uppercase;margin-top:4px;">Built by DBAs, for everyone.</div>
        </td></tr>

        <!-- Body -->
        <tr><td style="padding:40px 40px 32px;">
          <h1 style="margin:0 0 8px;font-size:1.5rem;font-weight:800;color:#0f172a;letter-spacing:-0.02em;">Your trial has started!</h1>
          <p style="margin:0 0 28px;color:#475569;font-size:0.95rem;line-height:1.6;">You have <strong>30 days</strong> to explore QueryAstra. Here&#39;s how to get the most out of it.</p>

          <!-- Steps -->
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border-radius:12px;padding:24px 28px;margin-bottom:28px;">
            <tr><td style="padding-bottom:16px;">
              <div style="font-size:0.75rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:16px;">Quick Start</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="width:32px;vertical-align:top;padding-bottom:14px;">
                    <div style="width:24px;height:24px;background:#7b2d8b;border-radius:50%;text-align:center;line-height:24px;font-size:0.75rem;font-weight:700;color:#fff;">1</div>
                  </td>
                  <td style="vertical-align:top;padding-bottom:14px;padding-left:12px;color:#0f172a;font-size:0.9rem;line-height:1.5;">
                    <strong>Add your first database instance</strong> &mdash; Settings &rarr; Instances &rarr; Add
                  </td>
                </tr>
                <tr>
                  <td style="width:32px;vertical-align:top;padding-bottom:14px;">
                    <div style="width:24px;height:24px;background:#7b2d8b;border-radius:50%;text-align:center;line-height:24px;font-size:0.75rem;font-weight:700;color:#fff;">2</div>
                  </td>
                  <td style="vertical-align:top;padding-bottom:14px;padding-left:12px;color:#0f172a;font-size:0.9rem;line-height:1.5;">
                    <strong>Explore the Query Performance Analyzer</strong> &mdash; see your top queries by CPU and execution time
                  </td>
                </tr>
                <tr>
                  <td style="width:32px;vertical-align:top;">
                    <div style="width:24px;height:24px;background:#7b2d8b;border-radius:50%;text-align:center;line-height:24px;font-size:0.75rem;font-weight:700;color:#fff;">3</div>
                  </td>
                  <td style="vertical-align:top;padding-left:12px;color:#0f172a;font-size:0.9rem;line-height:1.5;">
                    <strong>Set up an alert rule</strong> &mdash; get notified when something goes wrong
                  </td>
                </tr>
              </table>
            </td></tr>
          </table>

          <!-- CTA -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            <tr><td align="center">
              <a href="http://localhost:8124" style="display:inline-block;background:#7b2d8b;color:#ffffff;font-size:1rem;font-weight:700;text-decoration:none;padding:14px 36px;border-radius:10px;">
                Open QueryAstra &rarr;
              </a>
            </td></tr>
          </table>

          <!-- Note -->
          <p style="margin:0;font-size:0.82rem;color:#94a3b8;line-height:1.6;text-align:center;">
            When your trial ends you&#39;ll move to the Free plan (1 instance, 14-day history).<br>
            Upgrade anytime at <a href="https://queryastra.com/#pricing" style="color:#7b2d8b;">queryastra.com/#pricing</a>
          </p>
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:20px 40px 32px;border-top:1px solid #e2e8f0;text-align:center;">
          <p style="margin:0;font-size:0.78rem;color:#94a3b8;">QueryAstra &middot; <a href="mailto:support@queryastra.com" style="color:#94a3b8;">support@queryastra.com</a></p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _build_html(days_remaining: int) -> str:
    if days_remaining == 0:
        urgency = "Your trial <strong>expires today</strong>."
    elif days_remaining == 1:
        urgency = "Your trial expires <strong>tomorrow</strong>."
    else:
        urgency = f"Your trial expires in <strong>{days_remaining} days</strong>."

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>QueryAstra Trial Expiring</title>
</head>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;background:#111;border:1px solid #222;border-radius:12px;overflow:hidden;">
        <tr><td style="padding:28px 36px 20px;border-bottom:1px solid #1e1e1e;">
          <span style="font-size:20px;font-weight:700;color:#e5e5e5;letter-spacing:-0.3px;">QueryAstra</span>
        </td></tr>
        <tr><td style="padding:28px 36px;">
          <h1 style="margin:0 0 10px;font-size:18px;font-weight:600;color:#e5e5e5;">Trial Expiry Notice</h1>
          <p style="margin:0 0 20px;font-size:14px;color:#a0a0a0;line-height:1.65;">{urgency} Upgrade to a paid plan to keep full access to your monitored instances and all QueryAstra features.</p>
          <table cellpadding="0" cellspacing="0" style="margin:0 0 24px;">
            <tr><td style="background:#2563eb;border-radius:7px;padding:11px 24px;">
              <a href="https://queryastra.com/pricing" style="color:#fff;font-size:14px;font-weight:600;text-decoration:none;">Upgrade Now</a>
            </td></tr>
          </table>
          <p style="margin:0;font-size:13px;color:#666;line-height:1.5;">Questions? Contact us at <a href="mailto:support@queryastra.com" style="color:#60a5fa;text-decoration:none;">support@queryastra.com</a>.</p>
        </td></tr>
        <tr><td style="padding:14px 36px;border-top:1px solid #1e1e1e;">
          <p style="margin:0;font-size:11px;color:#444;text-align:center;">This message was sent by QueryAstra running on your server.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
