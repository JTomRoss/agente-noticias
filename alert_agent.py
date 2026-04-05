"""
Monitor de noticias para un portafolio: solo envía email si Claude marca alerta relevante.
"""

from __future__ import annotations

import html as html_module
import json
import os
import re
import smtplib
import sys
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import feedparser
import requests
from anthropic import Anthropic
from dotenv import load_dotenv

DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5"

# (ticker, nombre para búsqueda / contexto)
PORTFOLIO: list[tuple[str, str]] = [
    ("DJI", "Dow Jones"),
    ("SPY", "S&P 500 ETF SPDR"),
    ("QQQ", "Invesco QQQ Nasdaq"),
    ("NVDA", "NVIDIA"),
    ("MSFT", "Microsoft"),
    ("GOOG", "Alphabet Google"),
    ("META", "Meta Platforms"),
    ("ARM", "ARM Holdings"),
    ("AMD", "Advanced Micro Devices"),
    ("INTC", "Intel"),
    ("ORCL", "Oracle"),
    ("PLTR", "Palantir"),
    ("QUBT", "Quantum Computing"),
    ("IONQ", "IonQ"),
    ("NVO", "Novo Nordisk"),
    ("OSCR", "Oscar Health"),
    ("HIMS", "Hims and Hers Health"),
    ("SOFI", "SoFi Technologies"),
    ("CQQQ", "Invesco China Technology ETF"),
    ("GRAB", "Grab Holdings"),
    ("FINV", "FinVolution 360 Finance"),
    ("QFIN", "Qifu Technology 360 DigiTech"),
    ("GCT", "GigaCloud Technology"),
    ("JD", "JD.com"),
    ("MSTR", "MicroStrategy"),
    ("AVGO", "Broadcom"),
    ("CRWV", "CoreWeave"),
    ("OKLO", "Oklo"),
    ("EOSE", "Eos Energy Enterprises"),
    ("RKLB", "Rocket Lab"),
    ("PNG", "PNG"),
    ("IREN", "Iris Energy"),
    ("MU", "Micron Technology"),
    ("SNDK", "Sandisk"),
    ("INOD", "Innodata"),
    ("DT", "Dynatrace"),
]

RSS_FEEDS: list[tuple[str, str]] = [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml"),
]

NEWS_LOOKBACK_HOURS = 12
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PortfolioAlertBot/1.0; +https://github.com/)"
}
NEWS_BATCH_SIZE = 6


def _build_newsapi_query(batch: list[tuple[str, str]]) -> str:
    """OR de ticker y nombre de empresa por cada activo del lote."""
    bits: list[str] = []
    for t, name in batch:
        bits.append(t)
        if name and name.strip().upper() != t.upper():
            n = name.strip()
            bits.append(f'"{n}"' if " " in n else n)
    return " OR ".join(bits)


def load_env() -> dict[str, str]:
    load_dotenv()
    keys = [
        "ANTHROPIC_API_KEY",
        "NEWS_API_KEY",
        "GMAIL_USER",
        "GMAIL_PASSWORD",
        "EMAIL_DESTINO",
    ]
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Faltan variables de entorno: {', '.join(missing)}"
        )
    return {k: os.environ[k] for k in keys}


def _normalize_key(title: str, link: str) -> str:
    t = re.sub(r"\s+", " ", (title or "").strip().lower())
    u = (link or "").strip()
    return u if u else t


def fetch_newsapi_batches(api_key: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Varias consultas NewsAPI (OR de tickers por lote)."""
    errors: list[str] = []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    from_dt = (datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    url = "https://newsapi.org/v2/everything"

    batches: list[list[tuple[str, str]]] = []
    for i in range(0, len(PORTFOLIO), NEWS_BATCH_SIZE):
        batches.append(PORTFOLIO[i : i + NEWS_BATCH_SIZE])

    for batch in batches:
        q = _build_newsapi_query(batch)
        params = {
            "q": q,
            "from": from_dt,
            "sortBy": "relevancy",
            "language": "en",
            "pageSize": 50,
            "apiKey": api_key,
        }
        try:
            r = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=45)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            errors.append(f"NewsAPI HTTP ({batch[0][0]}…): {e}")
            continue
        except ValueError as e:
            errors.append(f"NewsAPI JSON ({batch[0][0]}…): {e}")
            continue

        if data.get("status") != "ok":
            errors.append(
                f"NewsAPI error: {data.get('message', data.get('code', data))}"
            )
            continue

        for a in data.get("articles") or []:
            title = (a.get("title") or "").strip()
            link = (a.get("url") or "").strip()
            if not title or "[Removed]" in title:
                continue
            k = _normalize_key(title, link)
            if k in seen:
                continue
            seen.add(k)
            items.append(
                {
                    "titulo": title,
                    "url": link,
                    "fuente": (a.get("source") or {}).get("name") or "NewsAPI",
                    "fecha": a.get("publishedAt") or "",
                    "origen": "newsapi",
                }
            )

    return items, errors


def fetch_rss_feeds() -> tuple[list[dict[str, Any]], list[str]]:
    """Descarga y parsea feeds RSS/Atom."""
    errors: list[str] = []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS * 2)

    for label, feed_url in RSS_FEEDS:
        try:
            r = requests.get(feed_url, headers=HTTP_HEADERS, timeout=45)
            r.raise_for_status()
            parsed = feedparser.parse(r.content)
        except requests.RequestException as e:
            errors.append(f"RSS {label}: descarga {e}")
            continue
        except Exception as e:
            errors.append(f"RSS {label}: {e}")
            continue

        if getattr(parsed, "bozo", False) and not parsed.entries:
            errors.append(f"RSS {label}: feed mal formado o vacío")
            continue

        for entry in parsed.entries:
            title = (
                entry.get("title")
                or entry.get("summary")
                or ""
            )
            title = html_module.unescape(str(title).strip())
            link = ""
            if entry.get("link"):
                link = str(entry["link"]).strip()
            elif entry.get("links"):
                for l in entry["links"]:
                    if l.get("rel") == "alternate" and l.get("href"):
                        link = str(l["href"]).strip()
                        break
            published = ""
            for key in ("published", "updated", "pubDate"):
                if entry.get(key):
                    published = str(entry[key])
                    break
            if not title:
                continue

            struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if struct:
                try:
                    dt = datetime(*struct[:6], tzinfo=timezone.utc)
                    if dt < cutoff:
                        continue
                except (TypeError, ValueError):
                    pass

            k = _normalize_key(title, link)
            if k in seen:
                continue
            seen.add(k)
            items.append(
                {
                    "titulo": title,
                    "url": link,
                    "fuente": label,
                    "fecha": published,
                    "origen": "rss",
                }
            )

    return items, errors


def build_claude_evaluation_payload(
    headlines: list[dict[str, Any]],
    fetch_errors: list[str],
) -> str:
    portfolio_lines = [f"- {t}: {name}" for t, name in PORTFOLIO]
    return f"""Portafolio a vigilar (ticker y nombre):
{chr(10).join(portfolio_lines)}

Criterios de ALERTA (solo si impacta de verdad al portafolio o al mercado relevante):
- Movimientos de mercado mayores al ~2% o volatilidad extrema en índices/sectores ligados a estos activos
- Resultados corporativos, guidance, M&A o eventos materiales de empresas del portafolio
- Decisiones o comunicados de la Reserva Federal / política monetaria
- Geopolítica o macro que afecte claramente tecnología, semis, China tech, crypto-adjacent, salud tech, etc.
- Hacks, incidentes de seguridad o regulación relevante para crypto/tech/fintech

Titulares recientes (JSON). Cada objeto tiene titulo, url, fuente, fecha, origen.
Errores al obtener datos (si hay): {json.dumps(fetch_errors, ensure_ascii=False)}

{json.dumps(headlines[:200], ensure_ascii=False, indent=2)}

Responde ÚNICAMENTE con un JSON válido (sin markdown, sin ```), con esta forma exacta:
{{
  "alert": true o false,
  "tema_asunto": "frase corta para el asunto del email, sin emojis, máximo 70 caracteres",
  "resumen_ejecutivo_html": "fragmento HTML: uno o dos párrafos <p>...</p> en español",
  "items": [
    {{
      "tickers": ["NVDA", "AMD"],
      "titulo": "texto del titular o parafraseo breve",
      "url": "https://...",
      "motivo": "una frase en español sobre por qué importa"
    }}
  ]
}}

Si nada es suficientemente relevante para disparar alerta, usa alert: false, items: [], resumen_ejecutivo_html: "".
Si alert es true, debe haber al menos un item con tickers del portafolio que apliquen."""


def call_claude_decide(api_key: str, prompt: str) -> tuple[dict[str, Any] | None, str | None]:
    model = os.getenv("ANTHROPIC_MODEL", DEFAULT_CLAUDE_MODEL)
    try:
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        return None, f"Claude API: {e}\n{traceback.format_exc()}"

    parts: list[str] = []
    for block in msg.content:
        if hasattr(block, "text") and block.text:
            parts.append(block.text)
    raw = "\n".join(parts).strip()
    if not raw:
        return None, "Claude devolvió texto vacío."

    text = raw
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        while lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        return None, f"JSON de Claude inválido: {e}\nTexto: {text[:800]}"

    if not isinstance(data, dict):
        return None, "Claude no devolvió un objeto JSON."
    return data, None


def sanitize_subject_fragment(s: str, max_len: int = 70) -> str:
    s = re.sub(r"[\r\n]+", " ", (s or "").strip())
    s = re.sub(r"\s+", " ", s)
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s or "Actualización"


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "si"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False
    return False


def build_alert_email_html(
    decision: dict[str, Any],
    fetch_errors: list[str],
) -> str:
    summary = decision.get("resumen_ejecutivo_html") or ""
    items = decision.get("items") or []
    err_block = ""
    if fetch_errors:
        esc = html_module.escape(" | ".join(fetch_errors))
        err_block = f'<p style="color:#92400e;font-size:13px;"><strong>Avisos técnicos:</strong> {esc}</p>'

    parts_items: list[str] = ["<h2 style=\"margin-top:24px;\">Noticias relevantes</h2><ul style=\"line-height:1.6;\">"]
    for it in items:
        if not isinstance(it, dict):
            continue
        tickers = it.get("tickers") or []
        ts = ", ".join(str(x) for x in tickers if x)
        ts_html = html_module.escape(ts) if ts else ""
        titulo = str(it.get("titulo") or "")
        url = str(it.get("url") or "").strip()
        motivo = str(it.get("motivo") or "")
        t_esc = html_module.escape(titulo)
        m_esc = html_module.escape(motivo)
        if url:
            u_esc = html_module.escape(url, quote=True)
            title_link = f'<a href="{u_esc}" style="color:#1d4ed8;font-weight:600;">{t_esc}</a>'
        else:
            title_link = f"<strong>{t_esc}</strong>"
        badge = (
            f'<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:700;">{ts_html}</span> '
            if ts_html
            else ""
        )
        parts_items.append(
            f"<li style=\"margin-bottom:16px;\">{badge}{title_link}"
            f'<br/><span style="color:#4b5563;font-size:14px;">{m_esc}</span></li>'
        )
    parts_items.append("</ul>")

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Alerta portafolio</title>
</head>
<body style="font-family:Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.5;color:#111;max-width:720px;">
<h1 style="font-size:1.25em;color:#991b1b;">Alerta de portafolio</h1>
{err_block}
<div style="margin:16px 0;">{summary}</div>
{"".join(parts_items)}
<hr style="margin-top:28px;border:none;border-top:1px solid #e5e7eb;" />
<p style="font-size:12px;color:#6b7280;">Generado: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
</body>
</html>"""


def send_email_html(
    gmail_user: str,
    gmail_password: str,
    destino: str,
    subject: str,
    html_body: str,
) -> tuple[bool, str | None]:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = gmail_user
        msg["To"] = destino
        msg.attach(
            MIMEText(
                "Este mensaje requiere HTML. Abre el correo en un cliente compatible.",
                "plain",
                "utf-8",
            )
        )
        part_html = MIMEText(html_body, "html", "utf-8")
        part_html.set_charset("utf-8")
        msg.attach(part_html)

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=120) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, [destino], msg.as_string())
        return True, None
    except Exception as e:
        return False, f"SMTP: {e}\n{traceback.format_exc()}"


def run() -> int:
    try:
        env = load_env()
    except Exception as e:
        print(f"Error de configuración: {e}", file=sys.stderr)
        return 1

    fetch_errors: list[str] = []

    print("Obteniendo noticias NewsAPI (lotes por ticker)…")
    try:
        n_items, n_err = fetch_newsapi_batches(env["NEWS_API_KEY"])
        fetch_errors.extend(n_err)
        print(f"  NewsAPI: {len(n_items)} titulares únicos.")
    except Exception as e:
        err = f"NewsAPI fatal: {e}"
        fetch_errors.append(err)
        print(err, file=sys.stderr)
        n_items = []

    print("Obteniendo RSS (Reuters, CNBC, Fed)…")
    try:
        r_items, r_err = fetch_rss_feeds()
        fetch_errors.extend(r_err)
        print(f"  RSS: {len(r_items)} entradas.")
    except Exception as e:
        err = f"RSS fatal: {e}"
        fetch_errors.append(err)
        print(err, file=sys.stderr)
        r_items = []

    headlines = n_items + r_items
    if not headlines:
        print("Sin titulares; no se llama a Claude ni se envía email.")
        return 0

    print(f"Total titulares a evaluar: {len(headlines)}. Consultando a Claude…")
    prompt = build_claude_evaluation_payload(headlines, fetch_errors)
    decision, cerr = call_claude_decide(env["ANTHROPIC_API_KEY"], prompt)
    if cerr:
        print(cerr, file=sys.stderr)
        return 1

    assert decision is not None
    print("Respuesta JSON de Claude:")
    print(json.dumps(decision, ensure_ascii=False, indent=2))

    alert_flag = _coerce_bool(decision.get("alert"))
    items = decision.get("items")
    has_items = isinstance(items, list) and len(items) > 0
    should_send_alert = alert_flag and has_items

    if not should_send_alert:
        print("Sin alertas relevantes, no se envía email")
        return 0

    tema = sanitize_subject_fragment(str(decision.get("tema_asunto") or ""))
    subject = f"🚨 Alerta Portafolio - {tema}"
    html_body = build_alert_email_html(decision, fetch_errors)

    print("Alerta detectada, enviando email")
    print(f"Enviando alerta por email (asunto: {subject[:60]}…)…")
    ok, smtp_err = send_email_html(
        env["GMAIL_USER"],
        env["GMAIL_PASSWORD"],
        env["EMAIL_DESTINO"],
        subject,
        html_body,
    )
    if not ok:
        print(smtp_err or "Error SMTP", file=sys.stderr)
        return 1

    print("Alerta enviada correctamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
