"""
alerts.py - Alert system for deal notifications
Supports: console print, email (SMTP), and webhook (Slack/Discord/custom)
"""

import os
import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert formatters
# ---------------------------------------------------------------------------

def _format_deal_text(keyword: str, deals: list, threshold_gbp: float) -> str:
    lines = [
        f"DEAL ALERT — '{keyword}'",
        f"Threshold: £{threshold_gbp:.2f} GBP",
        f"Found {len(deals)} deals at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "-" * 60,
    ]
    for d in deals[:20]:  # cap at 20 in notifications
        lines.append(
            f"[{d.get('marketplace','?')} | {d.get('country','?')}]  "
            f"£{d.get('price_gbp','?'):.2f}  —  {d.get('title','?')[:60]}"
        )
        if d.get("url"):
            lines.append(f"  {d['url']}")
    return "\n".join(lines)


def _format_deal_html(keyword: str, deals: list, threshold_gbp: float) -> str:
    rows = ""
    for d in deals[:20]:
        rows += f"""
        <tr>
            <td>{d.get('marketplace','')}</td>
            <td>{d.get('country','')}</td>
            <td><strong>£{d.get('price_gbp', 0):.2f}</strong></td>
            <td>{d.get('title','')[:70]}</td>
            <td><a href="{d.get('url','')}">View →</a></td>
        </tr>"""
    return f"""
    <html><body>
    <h2>Deal Alert: <em>{keyword}</em></h2>
    <p>Found <strong>{len(deals)}</strong> listings at or below £{threshold_gbp:.2f} GBP</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:sans-serif;font-size:14px">
        <thead style="background:#f0f0f0">
            <tr><th>Marketplace</th><th>Country</th><th>Price (GBP)</th><th>Title</th><th>Link</th></tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <p style="color:#888;font-size:12px">Sent by Arbitrage Scout — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
    </body></html>"""


# ---------------------------------------------------------------------------
# Alert channels
# ---------------------------------------------------------------------------

def console_alert(keyword: str, deals: list, threshold_gbp: float, **_):
    """Always-on console output."""
    print("\n" + "=" * 60)
    print(_format_deal_text(keyword, deals, threshold_gbp))
    print("=" * 60 + "\n")


def email_alert(
    keyword: str,
    deals: list,
    threshold_gbp: float,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    to_email: str,
    from_email: Optional[str] = None,
    **_,
):
    """Send HTML email alert via SMTP (works with Gmail, Outlook, etc.)."""
    from_email = from_email or smtp_user
    subject = f"[Arbitrage Scout] {len(deals)} deals on '{keyword}' (≤£{threshold_gbp:.0f})"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(MIMEText(_format_deal_text(keyword, deals, threshold_gbp), "plain"))
    msg.attach(MIMEText(_format_deal_html(keyword, deals, threshold_gbp), "html"))

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) if smtp_port == 465 else smtplib.SMTP(smtp_host, smtp_port) as server:
            if smtp_port != 465:
                server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
        log.info(f"Email alert sent to {to_email} for '{keyword}'")
    except Exception as e:
        log.error(f"Email alert failed: {e}")


def webhook_alert(keyword: str, deals: list, threshold_gbp: float, webhook_url: str, **_):
    """
    POST a JSON payload to a webhook URL.
    Works with Slack incoming webhooks, Discord webhooks, Make/Zapier, etc.
    """
    import requests
    payload = {
        "text": _format_deal_text(keyword, deals, threshold_gbp),
        "keyword": keyword,
        "threshold_gbp": threshold_gbp,
        "deal_count": len(deals),
        "deals": [
            {
                "marketplace": d.get("marketplace"),
                "country": d.get("country"),
                "price_gbp": d.get("price_gbp"),
                "title": d.get("title", "")[:100],
                "url": d.get("url", ""),
            }
            for d in deals[:20]
        ],
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        log.info(f"Webhook alert posted ({resp.status_code}) for '{keyword}'")
    except Exception as e:
        log.error(f"Webhook alert failed: {e}")


# ---------------------------------------------------------------------------
# Alert manager — composable
# ---------------------------------------------------------------------------

class AlertManager:
    """
    Configure once. Supports batch fire() and single fire_single().
    """

    def __init__(self, db=None, config: Optional[dict] = None):
        self.db = db
        self.config = config or {}

    def _smtp_kwargs(self):
        return dict(
            smtp_host=self.config.get("smtp_host", "smtp.gmail.com"),
            smtp_port=self.config.get("smtp_port", 587),
            smtp_user=self.config.get("smtp_user", ""),
            smtp_password=self.config.get("smtp_password", ""),
            to_email=self.config.get("alert_email", ""),
        )

    def fire(self, keyword: str, deals: list, threshold_gbp: float):
        """Batch alert (legacy)."""
        console_alert(keyword, deals, threshold_gbp)
        if self.config.get("email_enabled") and deals:
            email_alert(keyword, deals, threshold_gbp, **self._smtp_kwargs())
        if self.config.get("webhook_url") and deals:
            webhook_alert(keyword, deals, threshold_gbp,
                          webhook_url=self.config["webhook_url"])

    def fire_single(self, keyword: str, listing: dict,
                    threshold, alert_type: str = "deal"):
        """Single-listing alert (used by the new pipeline)."""
        label = {
            "new_listing": "NEW LISTING",
            "price":       "PRICE ALERT",
            "deal":        "DEAL ALERT",
        }.get(alert_type, "ALERT")

        # Console
        p = listing.get("price_gbp")
        score = listing.get("deal_score", 0)
        pct = listing.get("pct_below_avg", 0)
        title = listing.get("title", "")[:60]
        market = listing.get("marketplace", "")
        print(f"\n{'='*60}")
        print(f"🔔 {label} — '{keyword}'")
        print(f"   {title}")
        print(f"   £{p:.2f} | {market} | score={score:.0f} | {pct:.1f}% below avg")
        if listing.get("url"):
            print(f"   {listing['url']}")
        print(f"{'='*60}\n")

        # Email
        if self.config.get("email_enabled"):
            email_alert(keyword, [listing], threshold or 0, **self._smtp_kwargs())

        # Webhook
        if self.config.get("webhook_url"):
            webhook_alert(keyword, [listing], threshold or 0,
                          webhook_url=self.config["webhook_url"])
