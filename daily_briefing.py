"""
Genera un briefing diario: precios (yfinance), noticias 24h (NewsAPI),
resumen con Claude y envío por Gmail (SMTP).
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
from difflib import SequenceMatcher
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import requests
import yfinance as yf
from anthropic import Anthropic
from dotenv import load_dotenv

# Modelo solicitado; alias documentado por Anthropic para Haiku 4.5
DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5"

ASSETS: list[tuple[str, str]] = [
    ("ES=F", "Futuros S&P 500"),
    ("^FTSE", "FTSE 100"),
    ("^N225", "Nikkei 225"),
    ("^HSI", "Hang Seng"),
    ("000001.SS", "Shanghai"),
    ("GC=F", "Oro"),
    ("CL=F", "Petróleo WTI"),
    ("BZ=F", "Petróleo Brent"),
    ("HG=F", "Cobre"),
    ("^IRX", "Bono EEUU 2A"),
    ("^TNX", "Bono EEUU 10A"),
    ("BTC-USD", "Bitcoin"),
    ("ETH-USD", "ETH/USD"),
    ("EURUSD=X", "EUR/USD"),
    ("USDCLP=X", "USD/CLP"),
]

# Consulta amplia; el filtro “últimas 24 h” se aplica en código (NewsAPI free suele exigir sortBy≠publishedAt).
NEWS_QUERY = (
    '("stock market" OR "financial markets" OR finance OR investing) OR '
    '("US economy" OR "Federal Reserve" OR "U.S. economic") OR '
    '("European economy" OR eurozone OR "ECB") OR '
    '("China economy" OR "Chinese economy" OR Beijing economy) OR '
    '("S&P 500" OR "S&P500" OR SPX) OR '
    '(cryptocurrency OR crypto OR bitcoin OR ethereum OR BTC OR ETH)'
)

NEWS_QUERY_FALLBACK = "stock market OR economy OR Federal Reserve OR bitcoin OR cryptocurrency"


def _parse_news_datetime(value: str) -> datetime | None:
    if not value or not value.strip():
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


# Subtítulos fijos (mismo texto en prompt, HTML de Claude y fallback)
NEWS_SECTION_HEADINGS: list[tuple[str, str]] = [
    (
        "economia",
        "🏦 Economía: bancos centrales, inflación, datos macroeconómicos",
    ),
    (
        "internacional",
        "🌍 Internacional: geopolítica, guerras, relaciones entre países",
    ),
    ("cripto", "₿ Cripto: bitcoin, ethereum, blockchain, hacks"),
    (
        "corporativo",
        "🏢 Corporativo: resultados de empresas, fusiones, nombramientos",
    ),
    ("mercados", "📊 Mercados: todo lo demás relevante para inversores"),
]


def _char_in_blocked_script(ch: str) -> bool:
    """True si la letra pertenece a un alfabeto que no es latino (inglés/español)."""
    if not ch.isalpha():
        return False
    cp = ord(ch)
    if 0x0370 <= cp <= 0x03FF:
        return True  # griego
    if 0x0400 <= cp <= 0x052F:
        return True  # cirílico
    if 0x0530 <= cp <= 0x058F:
        return True  # armenio
    if 0x0590 <= cp <= 0x05FF:
        return True  # hebreo
    if 0x0600 <= cp <= 0x06FF:
        return True  # árabe
    if 0x0700 <= cp <= 0x074F:
        return True  # siríaco
    if 0x0900 <= cp <= 0x0DFF:
        return True  # devanagari, bengalí, etc.
    if 0x0E00 <= cp <= 0x0E7F:
        return True  # tailandés
    if 0x3040 <= cp <= 0x30FF:
        return True  # hiragana/katakana
    if 0x4E00 <= cp <= 0x9FFF:
        return True  # CJK
    if 0xAC00 <= cp <= 0xD7AF:
        return True  # hangul
    if 0x0F00 <= cp <= 0x0FFF:
        return True  # tibetano
    if 0x1780 <= cp <= 0x17FF:
        return True  # jemer
    return False


# Letras típicas de otros idiomas latinos (no inglés ni español)
_NON_EN_ES_LATIN_LETTERS = frozenset(
    "ąćęłńśźżĄĆĘŁŃŚŹŻœŒøØæÆåÅßđħǎǐǒǔřžčšďťňŘŽČŠĎŤŇĽĹãõÃÕĸŋǵḥẽỹ"
)


def title_is_english_or_spanish_chars(title: str) -> bool:
    """
    Heurística por caracteres: acepta titulares en alfabeto latino típico de inglés/español.
    Rechaza otros alfabetos y marcas fuertes de portugués, polaco, alemán, nórdico, etc.
    """
    if not title or not str(title).strip():
        return False
    t = str(title).strip()
    letters = 0
    for ch in t:
        if ch in _NON_EN_ES_LATIN_LETTERS:
            return False
        if ch.isalpha():
            letters += 1
            if _char_in_blocked_script(ch):
                return False
            cp = ord(ch)
            if (0x0041 <= cp <= 0x005A) or (0x0061 <= cp <= 0x007A):
                continue
            if 0x00C0 <= cp <= 0x024F:
                continue
            return False
    return letters >= 3


def _normalize_title_for_similarity(title: str) -> str:
    t = title.lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def dedupe_news_by_title_similarity(
    items: list[dict[str, Any]], *, threshold: float = 0.82
) -> list[dict[str, Any]]:
    """Elimina titulares casi iguales (misma historia con distinta redacción)."""
    kept: list[dict[str, Any]] = []
    norms: list[str] = []
    for item in items:
        raw = (item.get("titular") or "").strip()
        norm = _normalize_title_for_similarity(raw)
        if len(norm) < 8:
            if norm in norms:
                continue
            norms.append(norm)
            kept.append(item)
            continue
        is_dup = False
        for prev in norms:
            if not prev:
                continue
            r = SequenceMatcher(None, norm, prev).ratio()
            if r >= threshold:
                is_dup = True
                break
        if not is_dup:
            norms.append(norm)
            kept.append(item)
    return kept


def filter_news_en_es_titles(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [it for it in items if title_is_english_or_spanish_chars(it.get("titular") or "")]


def _fallback_news_bucket(title: str) -> int:
    """Índice 0..4 en NEWS_SECTION_HEADINGS (heurística si Claude falla)."""
    t = (title or "").lower()
    if any(
        k in t
        for k in (
            "bitcoin",
            "ethereum",
            "crypto",
            "blockchain",
            "btc",
            "defi",
            "stablecoin",
            "token",
            "nft",
            "solana",
            "hack",
            "wallet",
            "coinbase",
            "binance",
        )
    ):
        return 2
    if any(
        k in t
        for k in (
            "fed ",
            "federal reserve",
            "ecb ",
            "boj ",
            "boe ",
            "rate cut",
            "rate hike",
            "interest rate",
            "inflation",
            " cpi",
            "cpi ",
            "ppi ",
            "gdp",
            "macro",
            "payroll",
            "jobs report",
            "unemployment",
            "consumer price",
            "retail sales",
        )
    ):
        return 0
    if any(
        k in t
        for k in (
            "war ",
            "ukraine",
            "russia",
            "putin",
            "zelensky",
            "china ",
            "taiwan",
            "iran",
            "israel",
            "gaza",
            "middle east",
            "nato",
            "sanction",
            "tariff",
            "trade war",
            "geopolit",
            "conflict",
            "military",
            "missile",
        )
    ):
        return 1
    if any(
        k in t
        for k in (
            "earnings",
            "quarterly",
            "q1 ",
            "q2 ",
            "q3 ",
            "q4 ",
            "revenue",
            "profit warning",
            "ceo ",
            "cfo ",
            "merger",
            "acquisition",
            "takeover",
            "buyout",
            "layoff",
            "stock split",
            "ipo",
            "names ",
            "appointed",
            "board ",
            "guidance",
        )
    ):
        return 3
    return 4


def build_news_fallback_html_sections(news: list[dict[str, Any]]) -> str:
    """Misma estructura de 5 bloques que pide el prompt, con heurística local."""
    parts: list[str] = [
        '<h2 style="margin:24px 0 12px 0;font-size:1.25em;">Noticias (respaldo automático)</h2>'
    ]
    if not news:
        for _key, heading in NEWS_SECTION_HEADINGS:
            parts.append(
                f'<h3 style="margin:18px 0 8px 0;font-size:1.1em;color:#1e3a5f;">'
                f"{html_module.escape(heading)}</h3>"
            )
            parts.append("<p><em>Sin titulares destacados en esta categoría.</em></p>")
        return "\n".join(parts)

    buckets: list[list[dict[str, Any]]] = [[] for _ in NEWS_SECTION_HEADINGS]
    for n in news:
        idx = _fallback_news_bucket(n.get("titular") or "")
        buckets[idx].append(n)
    for i, (_key, heading) in enumerate(NEWS_SECTION_HEADINGS):
        parts.append(
            f'<h3 style="margin:18px 0 8px 0;font-size:1.1em;color:#1e3a5f;">'
            f"{html_module.escape(heading)}</h3>"
        )
        group = buckets[i]
        if not group:
            parts.append("<p><em>Sin titulares destacados en esta categoría.</em></p>")
            continue
        parts.append('<ul style="margin:0 0 12px 0;padding-left:1.2em;line-height:1.55;">')
        for n in group[:25]:
            titular = n["titular"]
            t_esc = html_module.escape(titular)
            url = n.get("url") or ""
            if url:
                link = html_module.escape(url, quote=True)
                t_html = (
                    f'<a href="{link}" style="color:#1d4ed8;text-decoration:underline;">{t_esc}</a>'
                )
            else:
                t_html = t_esc
            src = html_module.escape(n.get("fuente") or "")
            meta = f" <span style='color:#6b7280;font-size:12px;'>({src})</span>" if src else ""
            parts.append(f"<li style='margin-bottom:8px;'>{t_html}{meta}</li>")
        parts.append("</ul>")
    return "\n".join(parts)


def load_env() -> dict[str, str]:
    """Carga variables desde .env y valida las obligatorias."""
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
            f"Faltan variables de entorno (defínelas en .env): {', '.join(missing)}"
        )
    return {k: os.environ[k] for k in keys}


def fetch_prices() -> tuple[list[dict[str, Any]], list[str]]:
    """
    Obtiene precio reciente y variación % respecto al cierre anterior disponible en el histórico.
    """
    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    for symbol, label in ASSETS:
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="10d", auto_adjust=True)
            if hist is None or hist.empty:
                errors.append(f"{label} ({symbol}): sin datos de histórico")
                continue

            closes = hist["Close"].dropna()
            if closes.empty:
                errors.append(f"{label} ({symbol}): columna Close vacía")
                continue

            last = float(closes.iloc[-1])
            if len(closes) >= 2:
                prev = float(closes.iloc[-2])
            else:
                prev = last

            if prev and prev != 0:
                pct = (last - prev) / prev * 100.0
            else:
                pct = 0.0

            rows.append(
                {
                    "activo": label,
                    "ticker": symbol,
                    "precio": round(last, 6),
                    "variacion_pct": round(pct, 4),
                }
            )
        except Exception as e:
            errors.append(f"{label} ({symbol}): {e}")

    return rows, errors


def _newsapi_fetch(
    api_key: str,
    q: str,
    *,
    sort_by: str,
    from_param: str | None,
    language: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Una petición a /v2/everything. Devuelve (data_json, error_str)."""
    url = "https://newsapi.org/v2/everything"
    params: dict[str, Any] = {
        "q": q,
        "sortBy": sort_by,
        "pageSize": 100,
        "apiKey": api_key,
    }
    if from_param:
        params["from"] = from_param
    if language:
        params["language"] = language

    try:
        resp = requests.get(url, params=params, timeout=60)
        body_preview = (resp.text or "")[:500]
        try:
            data = resp.json()
        except ValueError:
            return None, f"NewsAPI JSON inválido (HTTP {resp.status_code}): {body_preview}"

        if resp.status_code != 200:
            msg = data.get("message", data.get("code", resp.reason))
            return None, f"NewsAPI HTTP {resp.status_code}: {msg}"

        if data.get("status") != "ok":
            return None, f"NewsAPI error: {data.get('message', data.get('code', data))}"

        return data, None
    except requests.RequestException as e:
        return None, f"NewsAPI HTTP: {e}"


def get_news_lookback_hours() -> int:
    """
    Ventana de noticias en horas.
    - Variable NEWS_LOOKBACK_HOURS (entero) si está definida.
    - Lunes en UTC: 72 h (3 días) para cubrir fin de semana.
    - Resto de días: 24 h.
    """
    raw = (os.getenv("NEWS_LOOKBACK_HOURS") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), 168))
    if datetime.now(timezone.utc).weekday() == 0:
        return 72
    return 24


def fetch_news_24h(
    api_key: str, lookback_hours: int | None = None
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """
    Artículos en la ventana lookback_hours (filtrado local).
    En plan gratuito, sortBy=publishedAt suele fallar; usamos relevancy/popularity.
    """
    if lookback_hours is None:
        lookback_hours = get_news_lookback_hours()

    errors: list[str] = []
    meta: dict[str, Any] = {
        "intentos": [],
        "lookback_hours": lookback_hours,
    }

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    # Margen para el parámetro `from` de NewsAPI
    from_days_back = max(2, (lookback_hours + 23) // 24 + 1)
    from_day = (now - timedelta(days=from_days_back)).strftime("%Y-%m-%d")

    # 1) relevancy + from (2 días) + sin language (más resultados)
    data, err = _newsapi_fetch(
        api_key,
        NEWS_QUERY,
        sort_by="relevancy",
        from_param=from_day,
        language=None,
    )
    meta["intentos"].append({"q": "principal", "sortBy": "relevancy", "from": from_day, "error": err})

    if err:
        errors.append(err)
        data = None

    raw: list[dict[str, Any]] = []
    if data:
        raw = list(data.get("articles") or [])
        meta["totalResults_api"] = data.get("totalResults")

    # 2) Si no hay crudo, probar popularity
    if not raw:
        data2, err2 = _newsapi_fetch(
            api_key,
            NEWS_QUERY,
            sort_by="popularity",
            from_param=from_day,
            language=None,
        )
        meta["intentos"].append(
            {"q": "principal", "sortBy": "popularity", "from": from_day, "error": err2}
        )
        if err2:
            errors.append(err2)
        elif data2:
            raw = list(data2.get("articles") or [])
            meta["totalResults_api"] = data2.get("totalResults")

    # 3) Consulta corta de respaldo
    if not raw:
        data3, err3 = _newsapi_fetch(
            api_key,
            NEWS_QUERY_FALLBACK,
            sort_by="relevancy",
            from_param=from_day,
            language="en",
        )
        meta["intentos"].append({"q": "fallback", "sortBy": "relevancy", "error": err3})
        if err3:
            errors.append(err3)
        elif data3:
            raw = list(data3.get("articles") or [])
            meta["totalResults_api"] = data3.get("totalResults")

    articles: list[dict[str, Any]] = []
    seen: set[str] = set()
    skipped_old = 0

    for a in raw:
        title = (a.get("title") or "").strip()
        link = (a.get("url") or "").strip()
        pub = (a.get("publishedAt") or "").strip()
        if "[Removed]" in title or title.lower() == "[removed]":
            continue

        dt = _parse_news_datetime(pub)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt < cutoff:
                skipped_old += 1
                continue

        key = link or title
        if not title or key in seen:
            continue
        seen.add(key)
        articles.append(
            {
                "titular": title,
                "fuente": (a.get("source") or {}).get("name") or "",
                "url": link,
                "fecha": pub,
            }
        )

    meta["recibidos_crudos"] = len(raw)
    meta["filtrados_mas_24h"] = skipped_old
    meta["tras_filtro"] = len(articles)

    if not raw and not errors:
        errors.append("NewsAPI: 0 artículos en la respuesta (revisa clave/plan y consulta).")

    # Si la API trae notas pero ninguna pasa el corte de la ventana (zonas horarias / retraso), usar recientes sin filtro.
    if not articles and raw:
        meta["relajado_sin_filtro_24h"] = True
        seen2: set[str] = set()
        for a in raw:
            title = (a.get("title") or "").strip()
            link = (a.get("url") or "").strip()
            if "[Removed]" in title or not title:
                continue
            key = link or title
            if key in seen2:
                continue
            seen2.add(key)
            articles.append(
                {
                    "titular": title,
                    "fuente": (a.get("source") or {}).get("name") or "",
                    "url": link,
                    "fecha": a.get("publishedAt") or "",
                }
            )
            if len(articles) >= 40:
                break
        errors.append(
            f"NewsAPI: ninguna noticia encajó en las últimas {lookback_hours} h exactas; "
            "se listan las más recientes devueltas por la API (pueden superar esa ventana)."
        )

    return articles, errors, meta


def _format_price_display(p: float) -> str:
    ax = abs(p)
    if ax >= 1000:
        return f"{p:,.2f}"
    if ax >= 1:
        return f"{p:,.4f}"
    return f"{p:.6f}".rstrip("0").rstrip(".")


def build_prices_table_html(rows: list[dict[str, Any]], price_errors: list[str]) -> str:
    """Tabla HTML de precios con verde/rojo según variación."""
    parts: list[str] = [
        '<h2 style="margin:0 0 12px 0;font-size:1.35em;">Precios de mercado</h2>',
        '<table style="border-collapse:collapse;width:100%;max-width:720px;font-size:14px;'
        'box-shadow:0 1px 4px rgba(0,0,0,.08);border-radius:8px;overflow:hidden;">',
        "<thead><tr>"
        '<th style="text-align:left;padding:10px 12px;background:#1a365d;color:#fff;">Activo</th>'
        '<th style="text-align:right;padding:10px 12px;background:#1a365d;color:#fff;">Precio</th>'
        '<th style="text-align:right;padding:10px 12px;background:#1a365d;color:#fff;">Variación</th>'
        "</tr></thead><tbody>",
    ]

    for i, r in enumerate(rows):
        pct = float(r["variacion_pct"])
        if pct > 0:
            color = "#166534"
            bg = "#ecfdf5"
            sign = "+"
        elif pct < 0:
            color = "#991b1b"
            bg = "#fef2f2"
            sign = ""
        else:
            color = "#374151"
            bg = "#f9fafb"
            sign = ""

        row_bg = bg if i % 2 == 0 else "#ffffff"
        precio = _format_price_display(float(r["precio"]))
        var_str = f"{sign}{pct:.2f}%"
        parts.append(
            f'<tr style="background:{row_bg};">'
            f'<td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;">'
            f'{html_module.escape(r["activo"])}'
            f'<span style="color:#6b7280;font-size:12px;"> ({html_module.escape(r["ticker"])})</span></td>'
            f'<td style="text-align:right;padding:10px 12px;border-bottom:1px solid #e5e7eb;">{precio}</td>'
            f'<td style="text-align:right;padding:10px 12px;border-bottom:1px solid #e5e7eb;'
            f'font-weight:600;color:{color};">{html_module.escape(var_str)}</td>'
            "</tr>"
        )

    parts.append("</tbody></table>")

    if price_errors:
        parts.append(
            "<p style='margin-top:14px;font-size:13px;color:#92400e;'><strong>Avisos:</strong> "
            + html_module.escape(" | ".join(price_errors))
            + "</p>"
        )

    return "\n".join(parts)


def build_claude_prompt_news_only(news: list[dict[str, Any]], news_errors: list[str]) -> str:
    """Solo noticias: la tabla de precios la genera el script (HTML fiable)."""
    headings_lines = "\n".join(
        f'<h3>{html_module.escape(h)}</h3>' for _k, h in NEWS_SECTION_HEADINGS
    )
    payload = {
        "noticias": news[:80],
        "errores_noticias": news_errors,
    }
    return f"""Eres un editor de briefing financiero. Tienes titulares en JSON (campo "noticias"). No inventes noticias nuevas.

{json.dumps(payload, ensure_ascii=False, indent=2)}

REGLAS OBLIGATORIAS (aplícalas en este orden):
1) Idioma: descarta mentalmente cualquier titular que NO esté en inglés o en español. (El JSON ya viene prefiltrado por script latino EN/ES; si ves alguno dudoso, descártalo.)
2) Duplicados: si dos o más titulares cuentan la misma noticia con redacción parecida, conserva solo UNO (el más claro o completo).
3) Clasificación: asigna cada noticia a UNA sola categoría. Si varias encajan, elige la más específica. Si ninguna encaja bien, usa "📊 Mercados".
4) Orden fijo: debes generar EXACTAMENTE estas 5 secciones, en ESTE orden, sin omitir ninguna ni cambiar el texto del subtítulo (copia literal, incluidos emojis):

{headings_lines}

5) Formato HTML: después de cada <h3>, incluye un <ul> con un <li> por noticia.
   - OBLIGATORIO: si el objeto en JSON trae "url" no vacía, el titular (en español, puedes parafrasear) debe ir DENTRO de un enlace: <a href="URL_EXACTA_COPIADA_DEL_JSON" style="color:#1d4ed8;text-decoration:underline;">texto</a>. No acortes ni cambies la URL.
   - Si "url" viene vacía, usa solo texto plano en el <li> (sin <a>).
   - Tras el enlace puedes añadir la fuente en un <span style="color:#6b7280;font-size:12px;">(nombre fuente)</span> usando el campo "fuente" del JSON.
6) Si una categoría queda vacía tras filtrar, escribe debajo del <h3>: <p><em>Sin titulares destacados en esta categoría.</em></p>
7) Salida: devuelve ÚNICAMENTE el fragmento HTML (sin <!DOCTYPE>, sin <html>, sin <head>, sin <body>). Prohibido markdown, prohibido ```, prohibido JSON suelto.

Descripciones de sección (para clasificar):
- 🏦 Economía: bancos centrales, inflación, datos macroeconómicos
- 🌍 Internacional: geopolítica, guerras, relaciones entre países
- ₿ Cripto: bitcoin, ethereum, blockchain, hacks
- 🏢 Corporativo: resultados de empresas, fusiones, nombramientos
- 📊 Mercados: todo lo demás relevante para inversores"""


def summarize_with_claude(api_key: str, user_prompt: str) -> tuple[str | None, str | None]:
    """Llama a Claude y devuelve (html_fragment, error_message)."""
    model = os.getenv("ANTHROPIC_MODEL", DEFAULT_CLAUDE_MODEL)
    try:
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=8192,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return None, f"Claude API: {e}\n{traceback.format_exc()}"

    parts: list[str] = []
    try:
        for block in msg.content:
            if hasattr(block, "text") and block.text:
                parts.append(block.text)
    except Exception as e:
        return None, f"Claude respuesta inesperada: {e}"

    text = "\n".join(parts).strip()
    if not text:
        return None, "Claude devolvió contenido vacío."
    return text, None


def normalize_claude_html_fragment(raw: str) -> str | None:
    """Quita fences tipo ```html y descarta respuestas que parezcan JSON puro."""
    s = raw.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        if lines:
            lines = lines[1:]
        while lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()

    if not s:
        return None

    if s.startswith("{") and s.endswith("}"):
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return s
        if isinstance(parsed, dict) and any(
            k in parsed for k in ("noticias", "precios", "articles")
        ):
            return None
    return s


def compose_email_document(
    price_block: str,
    news_block: str,
    news_errors: list[str],
) -> str:
    """Documento HTML completo: tabla de precios (código) + bloque de noticias."""
    alerts: list[str] = []
    if news_errors:
        esc = html_module.escape("\n".join(news_errors))
        alerts.append(
            f"<p style='background:#fffbeb;border:1px solid #fcd34d;padding:12px;border-radius:8px;'>"
            f"<strong>Avisos NewsAPI:</strong><br>{esc.replace(chr(10), '<br>')}</p>"
        )
    alert_block = "\n".join(alerts)
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Daily briefing</title>
</head>
<body style="font-family:Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.5;color:#111;max-width:800px;">
{alert_block}
{price_block}
<hr style="margin:28px 0;border:none;border-top:1px solid #e5e7eb;" />
{news_block}
<hr style="margin-top:2em;border:none;border-top:1px solid #ccc;" />
<p style="font-size:0.85em;color:#666;">Generado: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
</body>
</html>"""


def send_email_html(
    gmail_user: str,
    gmail_password: str,
    destino: str,
    subject: str,
    html_body: str,
) -> tuple[bool, str | None]:
    """Envía correo multipart/alternative: texto plano mínimo + HTML (charset UTF-8)."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = gmail_user
        msg["To"] = destino

        part_plain = MIMEText(
            "Este mensaje es HTML. Abre el correo en Gmail (vista web) o en un cliente "
            "que muestre formato HTML.",
            "plain",
            "utf-8",
        )
        part_html = MIMEText(html_body, "html", "utf-8")
        part_html.set_charset("utf-8")

        msg.attach(part_plain)
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
        print(f"Error al cargar .env: {e}", file=sys.stderr)
        return 1

    print("Obteniendo precios…")
    prices, price_errors = fetch_prices()
    if price_errors:
        for err in price_errors:
            print(f"  [precios] {err}", file=sys.stderr)
    print(f"  OK: {len(prices)} activos con datos.")

    lookback_h = get_news_lookback_hours()
    print(f"Obteniendo noticias (ventana últimas {lookback_h} h)…")
    news, news_errors, news_meta = fetch_news_24h(env["NEWS_API_KEY"], lookback_h)
    if news_errors:
        for err in news_errors:
            print(f"  [noticias] {err}", file=sys.stderr)
    print(
        f"  OK: {len(news)} titulares tras filtro {lookback_h} h "
        f"(crudos API: {news_meta.get('recibidos_crudos', 0)})."
    )
    print(f"  Meta NewsAPI: {json.dumps(news_meta, ensure_ascii=False)}")

    pre_lang = len(news)
    news = filter_news_en_es_titles(news)
    print(f"  Tras filtro idioma EN/ES (por caracteres del título): {len(news)} (antes {pre_lang}).")
    pre_dedupe = len(news)
    news = dedupe_news_by_title_similarity(news)
    print(f"  Tras deduplicación por similitud de título: {len(news)} (antes {pre_dedupe}).")

    print("\n--- Noticias incluidas en el briefing (verificación consola) ---")
    if not news:
        print("  (ninguna)")
    else:
        for i, item in enumerate(news, 1):
            print(f"  {i}. {item.get('titular', '')}")
            if item.get("url"):
                print(f"      {item['url']}")
    print("--- Fin noticias ---\n")

    price_html = build_prices_table_html(prices, price_errors)

    prompt = build_claude_prompt_news_only(news, news_errors)
    api_key = env["ANTHROPIC_API_KEY"]
    preview = api_key[:20]
    print(
        f"Verificación ANTHROPIC_API_KEY (primeros 20 caracteres, repr): {preview!r} "
        f"(longitud total: {len(api_key)})"
    )
    print("Generando resumen de noticias con Claude…")
    raw_claude, claude_err = summarize_with_claude(api_key, prompt)
    news_html: str
    if claude_err:
        print(claude_err, file=sys.stderr)
        news_html = (
            "<p style='color:#92400e;margin:0 0 12px 0;'>No se pudo generar el resumen con Claude; "
            "se muestran los titulares agrupados automáticamente.</p>"
            + build_news_fallback_html_sections(news)
        )
    else:
        normalized = normalize_claude_html_fragment(raw_claude or "")
        if normalized:
            news_html = (
                '<h2 style="margin:24px 0 12px 0;font-size:1.25em;">Resumen de noticias</h2>'
                + normalized
            )
        else:
            print(
                "Claude devolvió contenido no HTML (p. ej. JSON); usando listado de titulares.",
                file=sys.stderr,
            )
            news_html = build_news_fallback_html_sections(news)

    subject_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"Daily briefing — {subject_date}"
    html = compose_email_document(price_html, news_html, news_errors)

    print("Enviando correo (HTML multipart)…")
    ok, smtp_err = send_email_html(
        env["GMAIL_USER"],
        env["GMAIL_PASSWORD"],
        env["EMAIL_DESTINO"],
        subject,
        html,
    )
    if not ok:
        print(smtp_err or "Error SMTP desconocido", file=sys.stderr)
        return 1

    print("Correo enviado correctamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
