"""
Genera un briefing financiero: precios (yfinance), noticias 24 h (NewsAPI + Apollo RSS + JPM vigilancia),
resumen con Claude y envío por Gmail (SMTP).

Modos:
  BRIEFING_MODE=diario     (default) — mañana, con tabla de precios
  BRIEFING_MODE=vespertino — tarde/6pm, sin tabla de precios, lookback ~11 h, sin vigilancia JPM
"""

from __future__ import annotations

import hashlib
import html as html_module
import json
import os
import re
import smtplib
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
from difflib import SequenceMatcher
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import requests
import yfinance as yf
from anthropic import Anthropic
from dotenv import load_dotenv

# Modelo solicitado; alias documentado por Anthropic para Haiku 4.5
DEFAULT_CLAUDE_MODEL = "claude-sonnet-4-6"

ASSETS: list[tuple[str, str]] = [
    ("ES=F", "Futuros S&P 500"),
    ("NQ=F", "Futuros Nasdaq"),
    ("USDCLP=X", "Dólar vs Peso Chileno"),
    ("CL=F", "Petróleo WTI"),
    ("BZ=F", "Petróleo Brent"),
    ("HG=F", "Cobre"),
    ("2YY=F", "Treasury 2 años"),
    ("^TNX", "Treasury 10 años"),
    ("BTC-USD", "Bitcoin"),
    ("GC=F", "Oro"),
]

# Símbolos cuyo valor de yfinance es una tasa en % (variación expresada en pb).
YIELD_SYMBOLS_PCT = frozenset({"2YY=F", "^TNX"})

# Consulta amplia; el filtro “últimas 24 h” se aplica en código (NewsAPI free suele exigir sortBy≠publishedAt).
NEWS_QUERY = (
    '("stock market" OR "financial markets" OR finance OR investing) OR '
    '("US economy" OR "Federal Reserve" OR "U.S. economic") OR '
    '("European economy" OR eurozone OR "ECB") OR '
    '("China economy" OR "Chinese economy" OR Beijing economy) OR '
    '("S&P 500" OR "S&P500" OR SPX) OR '
    '(cryptocurrency OR crypto OR bitcoin OR ethereum OR BTC OR ETH) OR '
    '(Trump AND (tariff OR trade OR market OR economy OR rate OR China OR Fed OR stock OR crypto OR sanction OR executive OR order))'
)

# Consulta dedicada a declaraciones / posts de Trump con impacto en mercados.
NEWS_QUERY_TRUMP = (
    '(Trump OR "Donald Trump") AND '
    '(tweet OR post OR "Truth Social" OR tariff OR sanction OR "executive order" OR '
    'China OR Fed OR rate OR market OR economy OR trade OR crypto OR wall OR budget)'
)

NEWS_QUERY_FALLBACK = "stock market OR economy OR Federal Reserve OR bitcoin OR cryptocurrency"

# Palabras en minúscula que identifican un titular como relacionado con Trump.
_TRUMP_KEYWORDS = frozenset(
    ["trump", "donald trump", "truth social", "@realdonaldtrump", "mar-a-lago"]
)

# ---------------------------------------------------------------------------
# Flash Report: watchlist family office, sectores prioritarios y fuentes Chile
# ---------------------------------------------------------------------------

# Empresas relacionadas al family office (cualquier noticia, grande o chica, se incluye).
FO_WATCHLIST: tuple[str, ...] = (
    "CMPC",
    "Colbún",
    "Banco Bice",
    "Banco Security",
    "Bice Vida",
    "Bicecorp",
    "Arauco",  # competencia directa
)

# Competidores internacionales del sector celulosa/madera (alcance global).
PULP_GLOBAL_WATCHLIST: tuple[str, ...] = (
    "Suzano",
    "Klabin",
    "UPM",
    "Stora Enso",
    "International Paper",
    "Eldorado Brasil",
)

# Google News RSS (sin API key). when:Xd limita la antigüedad.
GNEWS_RSS_BASE = "https://news.google.com/rss/search"

# --- Fuentes internacionales premium (whitelist) ---
# RSS directo (links limpios, tiempo real). label = nombre visible de la fuente.
PREMIUM_INTL_RSS: tuple[tuple[str, str], ...] = (
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html", "CNBC"),       # Markets
    ("https://www.cnbc.com/id/20910258/device/rss/rss.html", "CNBC"),        # Economy
    ("https://www.cnbc.com/id/10000664/device/rss/rss.html", "CNBC"),        # Economy/Finance
    ("https://feeds.marketwatch.com/marketwatch/topstories/", "MarketWatch"),
    ("https://feeds.marketwatch.com/marketwatch/marketpulse/", "MarketWatch"),
    ("https://finance.yahoo.com/news/rssindex", "Yahoo Finance"),
    ("https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "WSJ"),
    ("https://feeds.a.dj.com/rss/RSSWorldNews.xml", "WSJ"),
)

# Premium sin RSS abierto fiable → Google News restringido por dominio (links de Google, fuente confiable).
PREMIUM_INTL_GNEWS: tuple[tuple[str, str], ...] = (
    ('site:reuters.com (markets OR economy OR Fed OR stocks OR inflation) when:1d', "Reuters"),
    ('site:bloomberg.com (markets OR economy OR Fed OR stocks OR rates) when:1d', "Bloomberg"),
    ('site:ft.com (markets OR economy OR central bank OR equities) when:1d', "Financial Times"),
)

# Queries chilenas geo-localizadas (gl=CL en _gnews_rss_fetch ya sesga a fuentes locales
# reputadas: DF, La Tercera, Emol, etc.). No se usa site: porque Google News RSS lo combina
# mal con múltiples dominios + when:Xd y devuelve casi nada.
# when:3d (no 1d): un lunes, when:1d solo captura el domingo —día sin prensa financiera CL—
# y dejaba el bloque nacional vacío. 3 días cubre fin de semana y lunes; lo viejo (2+ días)
# el prompt lo manda al bloque "última semana", así que no ensucia las noticias del día.
# Big tech mega-caps: queries dedicadas restringidas a fuentes premium (US tech está
# masivamente indexado, site: aquí sí devuelve resultados de sobra). Se marcan intl_premium.
GNEWS_QUERIES_BIGTECH: tuple[tuple[str, str], ...] = (
    ('(Nvidia OR Microsoft OR Apple OR Alphabet OR Google OR Amazon OR Meta OR OpenAI OR Anthropic OR Broadcom OR Tesla) (site:reuters.com OR site:bloomberg.com OR site:cnbc.com OR site:wsj.com OR site:marketwatch.com) when:2d', "bigtech"),
)

# Cripto: solo si hay noticia real (no se fuerza). Restringida a premium.
GNEWS_QUERIES_CRYPTO: tuple[tuple[str, str], ...] = (
    ('(bitcoin OR ethereum OR "crypto") (site:reuters.com OR site:bloomberg.com OR site:cnbc.com OR site:coindesk.com) when:1d', "crypto"),
)

GNEWS_QUERIES_NACIONAL: tuple[tuple[str, str], ...] = (
    # (query, etiqueta para logs) — forma original que devolvió ~25 ítems/consulta
    ("economía OR mercados OR IPSA OR dólar OR IPC Chile when:3d", "chile_economia"),
    ("(Senado OR gobierno OR Hacienda OR reforma OR Presupuesto) Chile when:3d", "chile_politica"),
    ("(banco OR banca OR \"seguros de vida\" OR eléctrica OR energía) Chile when:3d", "chile_sectores"),
)

GNEWS_QUERIES_WATCHLIST: tuple[tuple[str, str], ...] = (
    ('"CMPC" OR "Colbún" OR "Banco Bice" OR "Bicecorp" OR "Banco Security" OR "Bice Vida" OR "Arauco" when:4d', "fo_watchlist"),
)

# Celulosa: restringida a fuentes premium e industria especializada (no medios menores).
# Celulosa global en inglés (en-US sesga a prensa internacional/industria y reduce el ruido
# en español que aparecía antes). Sin site: para no quedar en cero por la rigidez de Google News.
GNEWS_QUERIES_CELULOSA: tuple[tuple[str, str], ...] = (
    ('(pulp prices OR "market pulp" OR BHKP OR hardwood pulp) (Suzano OR Klabin OR UPM OR "Stora Enso" OR China OR Brazil) when:4d', "celulosa_global"),
)

GNEWS_QUERIES_HISTORICO_7D: tuple[tuple[str, str], ...] = (
    ("(IPC OR Imacec OR PIB OR \"Banco Central\" OR TPM OR reforma tributaria) Chile when:7d", "hist_chile"),
    ('(site:reuters.com OR site:bloomberg.com OR site:cnbc.com OR site:wsj.com) ("CPI" OR "Federal Reserve" OR "jobs report" OR GDP OR PCE) when:7d', "hist_eeuu"),
)

# RSS de Apollo Academy: un único <item> con varias entradas embebidas en HTML (Torsten Slok).
APOLLO_DAILY_SPARK_RSS = "https://www.apolloacademy.com/the-daily-spark/feed/"
CONTENT_ENCODED_NS = "{http://purl.org/rss/1.0/modules/content/}encoded"

# J.P. Morgan Asset Management (insights). Monitoreo por firma (HTML + model.json si existe).
JPM_AM_DEFAULT_WATCH_URLS: tuple[str, ...] = (
    "https://am.jpmorgan.com/gb/en/asset-management/per/insights/market-insights/market-updates/monthly-market-review/",
    "https://am.jpmorgan.com/gb/en/asset-management/per/insights/portfolio-insights/asset-class-views/fixed-income-views/",
    "https://am.jpmorgan.com/gb/en/asset-management/per/insights/portfolio-insights/asset-class-views/equity-views/",
    "https://am.jpmorgan.com/gb/en/asset-management/per/insights/market-insights/market-updates/the-weekly-brief/",
    "https://am.jpmorgan.com/gb/en/asset-management/per/insights/portfolio-insights/asset-class-views/asset-allocation-views/",
)
JPM_AM_STATE_FILENAME = "jpm_am_watch_state.json"
_JPM_SLUG_LABELS: dict[str, str] = {
    "monthly-market-review": "Monthly Market Review",
    "fixed-income-views": "Fixed Income Views",
    "equity-views": "Equity Views",
    "the-weekly-brief": "The Weekly Brief",
    "asset-allocation-views": "Asset Allocation Views",
}


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
    ("mercados", "📊 Mercados: flujos, valuación, renta fija, commodities"),
    (
        "economia",
        "🏦 Economía: bancos centrales, inflación, datos macroeconómicos",
    ),
    (
        "internacional",
        "🌍 Internacional: geopolítica, guerras, aranceles, sanciones",
    ),
    (
        "corporativo",
        "🏢 Corporativo: resultados de empresas, fusiones, nombramientos",
    ),
    ("₿ cripto", "₿ Cripto: bitcoin, ethereum, blockchain, hacks"),
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
    return [
        it
        for it in items
        if it.get("apollo_daily_spark")
        or it.get("jpm_institutional")
        or it.get("trump_priority")
        or title_is_english_or_spanish_chars(it.get("titular") or "")
    ]


def _fallback_news_bucket(title: str) -> int:
    """Índice 0..4 en NEWS_SECTION_HEADINGS (heurística si Claude falla).
    Nuevo orden: mercados=0, economia=1, internacional=2, corporativo=3, cripto=4
    """
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
        return 4
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
        return 1
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
        return 2
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
    return 0


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
        def _fallback_inst_order(x: dict[str, Any]) -> tuple[int, str]:
            if x.get("apollo_daily_spark"):
                return (0, "")
            if x.get("trump_priority"):
                return (1, "")
            if x.get("jpm_institutional"):
                return (2, "")
            return (3, x.get("titular") or "")

        group_sorted = sorted(group, key=_fallback_inst_order)
        parts.append('<ul style="margin:0 0 12px 0;padding-left:1.2em;line-height:1.55;">')
        for n in group_sorted[:25]:
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



def get_briefing_mode() -> str:
    """'diario' (default, mañana con precios) o 'vespertino' (tarde sin precios)."""
    return (os.getenv("BRIEFING_MODE") or "diario").strip().lower()


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


def _ref_close_before(closes, ref_date) -> float | None:
    """Último cierre disponible en o antes de ref_date (para anclar MTD/YTD)."""
    try:
        sub = closes[closes.index.date <= ref_date]
        if not sub.empty:
            return float(sub.iloc[-1])
    except Exception:
        pass
    return None


def fetch_prices() -> tuple[list[dict[str, Any]], list[str]]:
    """
    Precio reciente + variación % vs cierre previo, y anclas MTD/YTD:
    - Para precios: variación % desde el cierre del último día del mes/año anterior.
    - Para tasas: NIVEL (%) al cierre del último día del mes/año anterior.
    """
    import datetime as _dt

    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    today = datetime.now(timezone.utc).date()
    fin_mes_prev = today.replace(day=1) - _dt.timedelta(days=1)   # último día del mes anterior
    fin_anio_prev = _dt.date(today.year - 1, 12, 31)              # 31-dic del año anterior

    for symbol, label in ASSETS:
        try:
            t = yf.Ticker(symbol)
            # 400 días para cubrir el cierre de fin de año anterior con holgura.
            hist = t.history(period="400d", auto_adjust=True)
            if hist is None or hist.empty:
                errors.append(f"{label} ({symbol}): sin datos de histórico")
                continue

            closes = hist["Close"].dropna()
            if closes.empty:
                errors.append(f"{label} ({symbol}): columna Close vacía")
                continue

            last = float(closes.iloc[-1])
            prev = float(closes.iloc[-2]) if len(closes) >= 2 else last

            es_tasa = symbol in YIELD_SYMBOLS_PCT
            if es_tasa:
                pct = (last - prev) * 100.0  # variación diaria en pb
            elif prev and prev != 0:
                pct = (last - prev) / prev * 100.0
            else:
                pct = 0.0

            ref_mes = _ref_close_before(closes, fin_mes_prev)
            ref_anio = _ref_close_before(closes, fin_anio_prev)

            if es_tasa:
                # Para tasas mostramos el NIVEL (%) en la fecha ancla, no la variación.
                mtd_val = round(ref_mes, 6) if ref_mes is not None else None
                ytd_val = round(ref_anio, 6) if ref_anio is not None else None
            else:
                mtd_val = round((last - ref_mes) / ref_mes * 100.0, 4) if ref_mes else None
                ytd_val = round((last - ref_anio) / ref_anio * 100.0, 4) if ref_anio else None

            rows.append(
                {
                    "activo": label,
                    "ticker": symbol,
                    "precio": round(last, 6),
                    "variacion_pct": round(pct, 4),
                    "es_tasa": es_tasa,
                    "mtd": mtd_val,
                    "ytd": ytd_val,
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
    - Modo vespertino: 11 h (desde el briefing de mañana).
    - Lunes en UTC (modo diario): 72 h para cubrir fin de semana.
    - Resto de días (modo diario): 24 h.
    """
    raw = (os.getenv("NEWS_LOOKBACK_HOURS") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), 168))
    if get_briefing_mode() == "vespertino":
        return 11
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



def _title_has_trump(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in _TRUMP_KEYWORDS)


def fetch_trump_news(
    api_key: str, lookback_hours: int
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch dedicado a noticias/tweets Trump con impacto en mercados."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    from_days_back = max(2, (lookback_hours + 23) // 24 + 1)
    from_day = (now - timedelta(days=from_days_back)).strftime("%Y-%m-%d")
    data, err = _newsapi_fetch(
        api_key,
        NEWS_QUERY_TRUMP,
        sort_by="relevancy",
        from_param=from_day,
        language=None,
    )
    if err:
        return [], err
    raw = list((data or {}).get("articles") or [])
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for a in raw:
        title = (a.get("title") or "").strip()
        link = (a.get("url") or "").strip()
        if "[Removed]" in title or not title:
            continue
        dt = _parse_news_datetime((a.get("publishedAt") or "").strip())
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt < cutoff:
                continue
        key = link or title
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "titular": title,
                "fuente": (a.get("source") or {}).get("name") or "",
                "url": link,
                "fecha": a.get("publishedAt") or "",
                "trump_priority": True,
            }
        )
    return items, None


def _apollo_rss_join_encoded_html(xml_text: str) -> str:
    """Concatena el HTML de todos los content:encoded (y description) del canal RSS."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return ""
    channel = root.find("channel")
    if channel is None:
        return ""
    parts: list[str] = []
    for item in channel.findall("item"):
        enc = item.find(CONTENT_ENCODED_NS)
        if enc is not None and (enc.text or "").strip():
            parts.append(enc.text or "")
        desc = item.find("description")
        if desc is not None and (desc.text or "").strip():
            parts.append(desc.text or "")
    return "\n".join(parts)


def _parse_apollo_daily_spark_from_html(
    encoded_html: str, *, cutoff_utc: datetime
) -> list[dict[str, Any]]:
    """
    Extrae entradas del listado embebido (li.wp-block-post + time + h2.spark-post-title).
    Orden del feed: la más reciente va primero.
    """
    chunks = re.split(r'(?=<li class="wp-block-post)', encoded_html)
    ordered: list[dict[str, Any]] = []
    for ch in chunks:
        if "spark-post-title" not in ch:
            continue
        m_time = re.search(r'<time datetime="([^"]+)"', ch)
        m_link = re.search(
            r'<h2[^>]*spark-post-title[^>]*>\s*<a href="([^"]+)"[^>]*>\s*([^<]*?)\s*</a>',
            ch,
            re.IGNORECASE | re.DOTALL,
        )
        if not m_link:
            continue
        url = (m_link.group(1) or "").strip()
        title = html_module.unescape((m_link.group(2) or "").strip())
        if not title or not url:
            continue
        if url.rstrip("/").endswith("/the-daily-spark"):
            continue
        pub_iso = (m_time.group(1) or "").strip() if m_time else ""
        dt_utc: datetime | None = None
        if pub_iso:
            try:
                dt_parsed = datetime.fromisoformat(pub_iso)
            except ValueError:
                dt_parsed = None
            if dt_parsed is not None:
                if dt_parsed.tzinfo is None:
                    dt_utc = dt_parsed.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = dt_parsed.astimezone(timezone.utc)
        ordered.append(
            {
                "titular": title,
                "fuente": "Apollo Daily Spark",
                "url": url,
                "fecha": pub_iso,
                "apollo_daily_spark": True,
                "_dt_utc": dt_utc,
            }
        )

    in_window: list[dict[str, Any]] = []
    for row in ordered:
        dtu = row.get("_dt_utc")
        if isinstance(dtu, datetime) and dtu >= cutoff_utc:
            clean = {k: v for k, v in row.items() if k != "_dt_utc"}
            in_window.append(clean)

    if not in_window and ordered:
        row0 = dict(ordered[0])
        row0.pop("_dt_utc", None)
        in_window = [row0]

    return in_window


def fetch_apollo_daily_spark(
    lookback_hours: int,
    *,
    feed_url: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Descarga el RSS de The Daily Spark y devuelve notas en la ventana lookback_hours (UTC).
    Si ninguna entra en la ventana pero hay listado, devuelve la entrada más reciente.
    """
    url = (feed_url or os.getenv("APOLLO_DAILY_SPARK_RSS") or "").strip() or APOLLO_DAILY_SPARK_RSS
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max(1, lookback_hours))
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DailyBriefingBot/1.0; +https://example.local)",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=45)
        if resp.status_code != 200:
            return [], f"Apollo Daily Spark RSS HTTP {resp.status_code}"
        blob = _apollo_rss_join_encoded_html(resp.text)
        if not blob.strip():
            return [], "Apollo Daily Spark: RSS sin HTML reconocible (content:encoded vacío)."
        items = _parse_apollo_daily_spark_from_html(blob, cutoff_utc=cutoff)
        return items, None
    except requests.RequestException as e:
        return [], f"Apollo Daily Spark RSS: {e}"


def _jpm_html_text_fingerprint(html: str, max_chars: int = 12000) -> str:
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", s)
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_chars]


def _jpm_parse_title_description(html: str) -> tuple[str, str]:
    title = ""
    m = re.search(r"<title>\s*([\s\S]*?)\s*</title>", html, re.IGNORECASE)
    if m:
        title = html_module.unescape(re.sub(r"\s+", " ", m.group(1)).strip())
        for suf in (" | J.P. Morgan Asset Management", "| J.P. Morgan Asset Management"):
            if title.endswith(suf):
                title = title[: -len(suf)].strip()
    desc = ""
    m2 = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
    if m2:
        desc = html_module.unescape(m2.group(1).strip())
    return title, desc


def _jpm_model_json_url(html: str) -> str | None:
    m = re.search(r'(/content/jpm-am-aem[^\s"<>]+\.model\.json)', html)
    if not m:
        return None
    return "https://am.jpmorgan.com" + m.group(1)


def _jpm_page_signature(
    session: requests.Session, html: str
) -> tuple[str, str, str | None]:
    """Devuelve (sha256_hex, titular_desde_página, error_opcional)."""
    title, desc = _jpm_parse_title_description(html)
    headline = (title or "").strip()
    model_url = _jpm_model_json_url(html)
    if model_url:
        try:
            r = session.get(model_url, timeout=50)
            if r.status_code == 200 and r.content:
                return hashlib.sha256(r.content).hexdigest(), headline, None
        except requests.RequestException:
            pass
    body_fp = _jpm_html_text_fingerprint(html)
    blob = f"{headline}|{desc}|{body_fp}"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest(), headline, None


def _jpm_short_label(page_url: str) -> str:
    path = urlparse(page_url).path.rstrip("/")
    slug = path.split("/")[-1].lower()
    return _JPM_SLUG_LABELS.get(slug, slug.replace("-", " ").title())


def _load_jpm_watch_state(path: str) -> dict[str, Any]:
    if not os.path.isfile(path):
        return {"version": 1, "urls": {}}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "urls": {}}
    if not isinstance(data, dict):
        return {"version": 1, "urls": {}}
    if not isinstance(data.get("urls"), dict):
        data["urls"] = {}
    return data


def _save_jpm_watch_state(path: str, data: dict[str, Any]) -> str | None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError as e:
        return str(e)
    return None


def fetch_jpm_am_watch_updates() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """
    Compara cada URL vigilada con jpm_am_watch_state.json (o JPM_AM_WATCH_STATE_PATH).
    Si es la primera vez que vemos la URL, o cambió la firma del contenido, devuelve un titular
    con enlace a la página (institucional).
    """
    meta: dict[str, Any] = {"urls_checked": 0, "n_new": 0, "state_path": "", "disabled": False}
    if (os.getenv("JPM_AM_WATCH") or "1").strip().lower() in ("0", "false", "no"):
        meta["disabled"] = True
        return [], [], meta

    raw = (os.getenv("JPM_AM_WATCH_URLS") or "").strip()
    if raw:
        urls = [p.strip() for p in raw.split(",") if p.strip()]
    else:
        urls = list(JPM_AM_DEFAULT_WATCH_URLS)

    seen_norm: set[str] = set()
    deduped: list[str] = []
    for u in urls:
        norm = u.rstrip("/").lower()
        if norm in seen_norm:
            continue
        seen_norm.add(norm)
        deduped.append(u if u.endswith("/") else u + "/")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    state_path = (os.getenv("JPM_AM_WATCH_STATE_PATH") or "").strip() or os.path.join(
        base_dir, JPM_AM_STATE_FILENAME
    )
    meta["state_path"] = state_path
    state = _load_jpm_watch_state(state_path)
    url_state: dict[str, Any] = state.setdefault("urls", {})

    errors: list[str] = []
    new_items: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (compatible; DailyBriefingBot/1.0; +https://example.local)"}
    )

    for raw_url in deduped:
        state_key = raw_url.rstrip("/").lower()
        label = _jpm_short_label(raw_url)
        try:
            r = session.get(raw_url, timeout=60)
        except requests.RequestException as e:
            errors.append(f"JPM AM watch ({label}): {e}")
            continue
        if r.status_code != 200:
            errors.append(f"JPM AM watch ({label}): HTTP {r.status_code}")
            continue
        meta["urls_checked"] += 1
        sig, headline, sig_err = _jpm_page_signature(session, r.text)
        if sig_err:
            errors.append(f"JPM AM watch ({label}): {sig_err}")
            continue
        if not sig:
            errors.append(f"JPM AM watch ({label}): firma vacía")
            continue

        prev = url_state.get(state_key)
        prev_sig = prev.get("signature") if isinstance(prev, dict) else None
        clean_h = (headline or "").strip() or label
        titular = f"J.P. Morgan AM — {label}: {clean_h}"
        if prev_sig is not None and prev_sig != sig:
            titular = f"{titular} — actualización"

        entry = {
            "titular": titular,
            "fuente": "J.P. Morgan Asset Management",
            "url": raw_url,
            "fecha": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "jpm_institutional": True,
        }

        emit_baseline = (os.getenv("JPM_AM_WATCH_EMIT_BASELINE") or "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        skip_baseline = (os.getenv("JPM_AM_WATCH_SILENT_BASELINE") or "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        is_github_ci = (os.getenv("GITHUB_ACTIONS") or "").strip().lower() == "true"
        if prev_sig is None:
            # Local: por defecto incluir una vez cada URL al iniciar seguimiento. En GitHub Actions
            # sin estado persistido, línea base silenciosa salvo JPM_AM_WATCH_EMIT_BASELINE=1.
            emit_first = emit_baseline or (not is_github_ci and not skip_baseline)
            if emit_first:
                new_items.append(entry)
            url_state[state_key] = {"signature": sig, "headline": clean_h, "page_url": raw_url}
        elif prev_sig != sig:
            new_items.append(entry)
            url_state[state_key] = {"signature": sig, "headline": clean_h, "page_url": raw_url}

    meta["n_new"] = len(new_items)
    save_err = _save_jpm_watch_state(state_path, state)
    if save_err:
        errors.append(f"JPM AM: no se pudo guardar estado ({state_path}): {save_err}")

    return new_items, errors, meta


def _format_price_display(p: float) -> str:
    ax = abs(p)
    if ax >= 1000:
        return f"{p:,.2f}"
    if ax >= 1:
        return f"{p:,.4f}"
    return f"{p:.6f}".rstrip("0").rstrip(".")


def build_prices_table_html(rows: list[dict[str, Any]], price_errors: list[str]) -> str:
    """Bloque monoespaciado de indicadores: precio, variación diaria, MTD y YTD.

    - Precios (acciones, commodities, cripto): MTD/YTD como variación % acumulada.
    - Tasas (Treasury 2A/10A): MTD/YTD como NIVEL (%) al cierre del mes/año anterior.
    """
    if not rows:
        return ""
    ancho_activo = max(len(r["activo"]) for r in rows) + 2

    def _fmt_diaria(r) -> tuple[str, float]:
        pct = float(r["variacion_pct"])
        if r.get("es_tasa"):
            if abs(pct) < 0.5:
                return "=", 0.0
            return f"{'+' if pct > 0 else ''}{pct:.0f} pb", pct
        return f"{'+' if pct > 0 else ''}{pct:.2f}%", pct

    def _fmt_acum(r, key) -> tuple[str, float]:
        """MTD/YTD: nivel para tasas, variación % para precios."""
        val = r.get(key)
        if val is None:
            return "n/d", 0.0
        if r.get("es_tasa"):
            return f"{float(val):.2f}%", 0.0  # nivel; sin color
        return f"{'+' if val > 0 else ''}{float(val):.2f}%", float(val)

    # Encabezado de columnas
    header = (
        f"{'':<{ancho_activo}}|  {'Precio':>11}  |  {'Día':>9}  |  {'MTD':>9}  |  {'YTD':>9}"
    )
    sep = "-" * len(header)

    lineas: list[str] = [html_module.escape(header), html_module.escape(sep)]
    for r in rows:
        precio = f"{float(r['precio']):.2f}%" if r.get("es_tasa") else f"{float(r['precio']):,.2f}"
        dia_str, dia_signo = _fmt_diaria(r)
        mtd_str, mtd_signo = _fmt_acum(r, "mtd")
        ytd_str, ytd_signo = _fmt_acum(r, "ytd")

        def _span(text, signo):
            color = "#dc2626" if signo < 0 else "#111111"
            return f'<span style="color:{color};font-weight:600;">{html_module.escape(f"{text:>9}")}</span>'

        activo = html_module.escape(f"{r['activo']:<{ancho_activo}}")
        precio_pad = html_module.escape(f"{precio:>11}")
        lineas.append(
            f"{activo}|  {precio_pad}  |  {_span(dia_str, dia_signo)}  |  "
            f"{_span(mtd_str, mtd_signo)}  |  {_span(ytd_str, ytd_signo)}"
        )

    parts = [
        '<pre style="font-family:Consolas,Menlo,monospace;font-size:12.5px;'
        'background:#f6f8fa;border:1px solid #e5e7eb;border-radius:8px;'
        'padding:14px 16px;overflow-x:auto;line-height:1.7;margin:0;">'
        + "\n".join(lineas)
        + "</pre>"
    ]
    if price_errors:
        parts.append(
            "<p style='margin-top:10px;font-size:13px;color:#92400e;'><strong>Avisos:</strong> "
            + html_module.escape(" | ".join(price_errors))
            + "</p>"
        )
    return "\n".join(parts)


def _gnews_rss_fetch(
    query: str, label: str, lang: str = "es-419", country: str = "CL"
) -> tuple[list[dict[str, Any]], str | None]:
    """Trae titulares desde Google News RSS (sin API key). Devuelve (items, error)."""
    params = {"q": query, "hl": lang, "gl": country, "ceid": f"{country}:{lang.split('-')[0]}"}
    try:
        resp = requests.get(GNEWS_RSS_BASE, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        return [], f"GoogleNews RSS ({label}): {e}"

    items: list[dict[str, Any]] = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        source_el = it.find("source")
        fuente = (source_el.text or "").strip() if source_el is not None else "Google News"
        if not title:
            continue
        # Google News antepone " - Fuente" al final del título; lo limpiamos.
        if fuente and title.endswith(f" - {fuente}"):
            title = title[: -(len(fuente) + 3)].strip()
        items.append(
            {
                "titular": title,
                "url": link,
                "fuente": fuente,
                "fecha": pub,
                "gnews_label": label,
            }
        )
    return items[:25], None


def _premium_rss_fetch(url: str, fuente: str) -> tuple[list[dict[str, Any]], str | None]:
    """Lee un RSS directo de una fuente premium (links limpios). Devuelve (items, error)."""
    try:
        resp = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        return [], f"{fuente} RSS: {e}"
    items: list[dict[str, Any]] = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        if not title:
            continue
        items.append(
            {
                "titular": title,
                "url": link,
                "fuente": fuente,
                "fecha": pub,
                "intl_premium": True,
            }
        )
    return items[:20], None


def fetch_premium_intl_news() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Noticias internacionales SOLO de fuentes premium (RSS directo + Google News por dominio)."""
    all_items: list[dict[str, Any]] = []
    errors: list[str] = []
    meta: dict[str, Any] = {}

    for url, fuente in PREMIUM_INTL_RSS:
        items, err = _premium_rss_fetch(url, fuente)
        if err:
            errors.append(err)
            continue
        all_items.extend(items)
        meta[fuente] = meta.get(fuente, 0) + len(items)

    for query, fuente in PREMIUM_INTL_GNEWS:
        items, err = _gnews_rss_fetch(query, f"premium_{fuente}", lang="en-US", country="US")
        if err:
            errors.append(err)
            continue
        for it in items:
            it["intl_premium"] = True
            it["fuente"] = fuente  # forzamos el nombre limpio de la fuente
        all_items.extend(items)
        meta[fuente] = meta.get(fuente, 0) + len(items)

    # Big tech y cripto: mantienen el nombre real de la fuente (vienen de varios premium).
    for queries, flag in ((GNEWS_QUERIES_BIGTECH, "bigtech"), (GNEWS_QUERIES_CRYPTO, "crypto")):
        for query, label in queries:
            items, err = _gnews_rss_fetch(query, label, lang="en-US", country="US")
            if err:
                errors.append(err)
                continue
            for it in items:
                it["intl_premium"] = True
                it[flag] = True
            all_items.extend(items)
            meta[label] = len(items)

    return all_items, errors, meta


def fetch_flash_report_sources() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """
    Trae las fuentes nuevas del Flash Report:
    - Noticias nacionales (Chile): economía, política y sectores prioritarios.
    - Watchlist family office (CMPC, Colbún, Bice, Security, Bice Vida, Arauco).
    - Celulosa global (Suzano, Klabin, demanda China/Europa, producción Brasil).
    - Históricos 7 días (Chile + EE.UU.) para los bloques "última semana".
    Cada item lleva flags para que el prompt los clasifique.
    """
    all_items: list[dict[str, Any]] = []
    errors: list[str] = []
    meta: dict[str, Any] = {}

    grupos: tuple[tuple[tuple[tuple[str, str], ...], dict[str, Any], str, str], ...] = (
        (GNEWS_QUERIES_NACIONAL, {"nacional": True}, "es-419", "CL"),
        (GNEWS_QUERIES_WATCHLIST, {"nacional": True, "fo_watchlist": True}, "es-419", "CL"),
        (GNEWS_QUERIES_CELULOSA, {"celulosa_global": True}, "en-US", "US"),
        (GNEWS_QUERIES_HISTORICO_7D, {"historico_7d": True}, "es-419", "CL"),
    )

    for queries, flags, lang, country in grupos:
        for q, label in queries:
            items, err = _gnews_rss_fetch(q, label, lang=lang, country=country)
            if err:
                errors.append(err)
                continue
            for it in items:
                it.update(flags)
                # El histórico de EE.UU. va al bloque internacional.
                if label == "hist_eeuu":
                    it.pop("nacional", None)
            meta[label] = len(items)
            all_items.extend(items)

    return all_items, errors, meta


def build_claude_prompt_news_only(
    news: list[dict[str, Any]],
    news_errors: list[str],
) -> str:
    """Prompt maestro del Flash Report: la tabla de indicadores la genera el script."""
    payload = {
        "noticias": news[:320],
        "errores_noticias": news_errors,
        "fecha_hoy": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
    return f"""Eres el editor de un briefing matinal de noticias económicas y financieras para un family office chileno sofisticado. Tu trabajo es SELECCIONAR y REDACTAR las noticias más relevantes del día a partir del JSON (campo "noticias"). Eres un editor con criterio, no un copista: decides qué entra y qué no. No inventes hechos: usa solo lo que viene en el JSON.

{json.dumps(payload, ensure_ascii=False, indent=2)}

=== PRINCIPIO RECTOR: NOTICIAS, NO COMENTARIOS ===
Cada ítem DEBE comunicar un HECHO CONCRETO del día: una cifra publicada, una decisión tomada, una operación anunciada, un movimiento de precio, un evento ocurrido. Si una frase no contiene un hecho verificable y solo describe un clima o una tendencia, NO va.
- PROHIBIDO (comentario vacío, esto NO es noticia): "La inflación en EE.UU. y su impacto en las tasas sigue siendo el factor dominante para los mercados de divisas." / "Las tendencias FX muestran que la inflación sigue siendo el driver principal." / "El mercado sigue los movimientos de oro y bonos ante la volatilidad."
- CORRECTO (hecho + dato): "El <strong>IPC</strong> de mayo en EE.UU. subió 0,5% MoM, sobre el 0,3% esperado, por el alza de energía." / "El <strong>S&P 500</strong> cerró -1,0% tras el dato de inflación, su mayor caída en tres semanas."
La redacción es editorial y fluida (no pegar el titular crudo), pero SIEMPRE anclada a un hecho con su cifra.

=== SELECCIÓN Y CALIDAD DE FUENTES ===
- INTERNACIONAL (Macro, Precios y Mercados, Política): usa EXCLUSIVAMENTE noticias con "intl_premium": true (Reuters, Bloomberg, CNBC, WSJ, MarketWatch, Yahoo Finance, Financial Times). JAMÁS uses una fuente chilena para una noticia internacional. Si una noticia internacional solo aparece en medios chilenos, descártala. ÚNICA excepción: el bloque de Celulosa usa las noticias con "celulosa_global": true (ver sección Celulosa).
- NACIONAL: usa las noticias con "nacional": true (medios chilenos reputados).
- Descarta cualquier ítem cuyo hecho no entiendas o que no aporte información real.
- Eres tú quien elige: prioriza lo que un inversor institucional necesita saber a primera hora, no rellenes con lo que sobra.

=== EXCLUSIONES (descarta ANTES de clasificar) ===
- Apuestas deportivas, deportes, farándula, entretenimiento, cultura pop, lifestyle.
- Consejos de inversión genéricos ("cómo invertir", "guía", "¿deberías comprar X?"), listas/rankings sin evento, clickbait.
- Opinión de pundits sin hecho concreto; política partidista sin impacto en mercados/tasas/reformas/regulación.
- Beneficios estatales y trámites para personas (bonos, subsidios, "consulta con tu RUT", fechas de pago, pensiones individuales). Descarta SIEMPRE.
- Consumo y operativa cotidiana (medios de pago en transporte/comercio, promociones a clientes, concursos, apps de consumo).
- En sectores prioritarios (banca, seguros de vida, energía) solo entra lo CORPORATIVO: resultados, M&A, inversiones, emisiones de deuda, regulación con impacto en empresas, nombramientos clave.
- EXCEPCIÓN ABSOLUTA: nunca descartes una noticia que mencione a {", ".join(FO_WATCHLIST)} — esas se incluyen SIEMPRE.

=== ESTRUCTURA DEL HTML (en este orden exacto) ===
(1) <h2>I. INTERNACIONAL</h2> con tres <h3>: "Macroeconomía", "Precios y Mercados", "Política Internacional".
(2) <hr style="border:none;border-top:1px solid #e5e7eb;margin:20px 0;">
(3) <h2>II. NACIONAL</h2> con tres <h3>: "Macroeconomía", "Precios y Mercados", "Política Nacional". Título "II. NACIONAL" a secas (sin "Chile").

=== QUÉ VA EN CADA SUBSECCIÓN ===
- MACROECONOMÍA: el DATO en sí. Cifra publicada vs. lo esperado, decisiones e intervenciones de bancos centrales (Fed, BCE, Banco Central de Chile). Ej: "El <strong>IPC</strong> de EE.UU. subió 0,5% MoM, sobre el 0,3% previsto." Las señales/decisiones/discursos de Fed y BCE son tan o más importantes que los datos: inclúyelas aquí.
- PRECIOS Y MERCADOS: la REACCIÓN de los activos a esos datos y los movimientos de mercado. Ej: "El <strong>dólar</strong> se debilitó frente a emergentes tras el dato de inflación; el peso chileno ganó terreno." Aquí van índices, acciones, commodities, divisas, tasas, resultados corporativos, hedge funds.
- POLÍTICA: hechos políticos con impacto económico (reformas, leyes, decisiones de gobierno, geopolítica).

=== REGLAS DE REDACCIÓN ===
- Máximo DOS líneas por noticia (~200 caracteres). Idea fuerza de inmediato.
- UNA SOLA palabra en negrita por noticia: la palabra clave de identificación rápida (el activo, el indicador o la entidad central). Nunca más de un <strong> por ítem. Ej: "El <strong>Banco Mundial</strong> recortó su proyección de crecimiento global a 2,5% por el impacto de la guerra en Irán."
- PROHIBIDO el formato "Etiqueta: explicación" con negrita y dos puntos (MAL: "<strong>Banca:</strong> el sector sube"). Redacta la idea directa.
- Datos económicos: SIEMPRE la métrica exacta + comparación contra lo esperado si está disponible.
- Consolida dos noticias del mismo tema en un solo ítem.
- Cada noticia es un <p style="margin:0 0 14px 0;line-height:1.55;">. NO uses viñetas salvo en el bloque de Celulosa (ver abajo).
- FUENTE: al final de cada noticia, un único enlace cuyo TEXTO es el nombre de la fuente: <a href="URL_EXACTA_DEL_JSON" style="color:#1d4ed8;text-decoration:none;">Fuente</a>. Nunca muestres la URL cruda. Si "url" viene vacía, cierra con <span style="color:#6b7280;font-size:12px;">(Fuente)</span>.

=== MACROECONOMÍA INTERNACIONAL: ORDEN ===
Agrupa por procedencia con prefijo en negrita, en este orden: <strong>EE.UU.:</strong>, <strong>China:</strong>, <strong>Europa:</strong> (o país específico como <strong>Alemania:</strong>). Lo que no calce con una geografía va al final SIN prefijo alguno. NUNCA escribas "Otros:" — ese prefijo está prohibido.

=== PRECIOS Y MERCADOS INTERNACIONAL ===
Es la subsección más rica del bloque internacional: puebla con índices, resultados corporativos, commodities, divisas, tasas y flujos (apunta a 4-6 noticias reales si el material existe). Orden: 1º EE.UU. de impacto sistémico (Fed, líderes, hedge funds); 2º corporativas/sectoriales EE.UU.; 3º Europa/Asia relevante; 4º Latinoamérica.
- BIG TECH (prioridad alta): incluye SIEMPRE que haya noticia relevante de las mega-caps tecnológicas —Nvidia, Microsoft, Apple, Alphabet/Google, Amazon, Meta, OpenAI, Anthropic, Broadcom, Tesla— por su peso en los índices. Son de las primeras en mostrarse dentro de las corporativas de EE.UU.
- CRIPTO (sin forzar): incluye bitcoin/ethereum/cripto SOLO si hay un hecho noticioso concreto del día (movimiento relevante, regulación, operación). Si no hay noticia real de cripto, NO inventes ni rellenes con generalidades; simplemente omítela.

=== CELULOSA (al final de Precios y Mercados Internacional) ===
Las noticias con "celulosa_global": true van en un bloque propio que abre con <p style="margin:14px 0 4px 0;"><strong>Celulosa</strong></p> seguido de una <ul style="margin:0;padding-left:20px;"> donde cada noticia es un <li style="margin-bottom:6px;"> de UNA línea con su enlace de fuente inmediatamente después del texto. Si no hay noticias de celulosa, omite el bloque completo.

=== NACIONAL ===
- Las noticias que mencionen a {", ".join(FO_WATCHLIST)} van PRIMERO en su subsección.
- Prioridad alta a banca, seguros de vida y energía (solo corporativo).
- Clasifica por contenido: declaración política de un ejecutivo → Política; resultado trimestral → Precios y Mercados.

=== ANTICIPOS / AGENDA (AL FINAL de cada subsección, nunca al inicio) ===
- Cierra Macroeconomía con una oración natural de lo que se publica hoy, si los titulares lo mencionan: "Hoy se conocerá el IPP de mayo en EE.UU. y las solicitudes semanales de desempleo." Sin etiqueta en negrita, sin dos puntos.
- Cierra Precios y Mercados con los resultados corporativos del día (foco tech S&P 500), si aplica: "Hoy reportan Adobe y Oracle tras el cierre." Va AL FINAL, nunca como primera noticia.

=== HISTÓRICO ÚLTIMA SEMANA ===
Al final de CADA subsección que tenga material de días previos muy relevante (noticias con "historico_7d": true: IPC, Imacec, PIB, TPM, reformas, CPI, jobs report, decisiones de bancos centrales), agrega:
<blockquote style="border-left:3px solid #d1d5db;margin:8px 0 4px 0;padding:6px 12px;background:#f9fafb;"><em>📅 Última semana: [resumen consolidado en cursiva, máximo 3 líneas, con enlaces de fuente]</em></blockquote>
Cada hecho histórico va en su subsección temática (un IPC pasado en Macroeconomía, no en Política). Si una noticia del día ya quedó vieja (publicada hace 2+ días), va aquí, no como noticia del día. Usa el material real del JSON; si para un bloque completo no hay ninguno, omite el blockquote (no inventes).
- GEOGRAFÍA EN EL HISTÓRICO: cuando todos los datos de un blockquote internacional son del mismo país (típicamente EE.UU.), indícalo UNA SOLA VEZ al inicio y no lo repitas en cada oración. Ej: "📅 Última semana (EE.UU.): el PCE core de abril marcó 3,3% anual; las nóminas ADP crecieron 122.000; las ofertas de empleo treparon a 7,6 millones." Si hay datos de varios países, especifica el país solo en los que no sean del país dominante.

=== FORMATO DE SALIDA ===
- Devuelve ÚNICAMENTE el fragmento HTML (sin <!DOCTYPE>, <html>, <head>, <body>). Prohibido markdown y bloques de código.
- Si una subsección queda sin noticias: <p><em>Sin titulares destacados.</em></p>.
- No agregues preguntas, menús ni cierres al final del reporte."""

def summarize_with_claude(api_key: str, user_prompt: str) -> tuple[str | None, str | None]:
    """Llama a Claude y devuelve (html_fragment, error_message)."""
    model = os.getenv("ANTHROPIC_MODEL", DEFAULT_CLAUDE_MODEL)
    try:
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=20000,
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


# Ventana del Mundial 2026 (11 jun - 19 jul). Fuera de estas fechas no se muestran partidos.
WORLDCUP_START = datetime(2026, 6, 11, tzinfo=timezone.utc).date()
WORLDCUP_END = datetime(2026, 7, 19, tzinfo=timezone.utc).date()
SANTIAGO_TZ = ZoneInfo("America/Santiago")


def _wc_from_football_data(today_cl, d_from: str, d_to: str) -> tuple[list[dict[str, str]], str | None]:
    """Intenta football-data.org con User-Agent y un reintento. (partidos, error)."""
    key = os.environ.get("FOOTBALL_DATA_KEY", "").strip()
    if not key:
        return [], "FOOTBALL_DATA_KEY no configurada"
    headers = {
        "X-Auth-Token": key,
        "User-Agent": "agente-noticias/1.0 (+https://github.com/JTomRoss/agente-noticias)",
        "Accept": "application/json",
    }
    last_err = None
    for intento in range(2):  # un reintento si la conexión se cae
        try:
            resp = requests.get(
                "https://api.football-data.org/v4/competitions/WC/matches",
                params={"dateFrom": d_from, "dateTo": d_to},
                headers=headers,
                timeout=25,
            )
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            last_err = e
            if intento == 0:
                time.sleep(2)
            else:
                return [], f"football-data.org: {e}"

    partidos: list[dict[str, str]] = []
    for m in data.get("matches", []):
        utc = m.get("utcDate")
        if not utc:
            continue
        try:
            dt_cl = datetime.fromisoformat(utc.replace("Z", "+00:00")).astimezone(SANTIAGO_TZ)
        except Exception:
            continue
        if dt_cl.date() != today_cl:
            continue
        home = (m.get("homeTeam") or {}).get("name") or "Por definir"
        away = (m.get("awayTeam") or {}).get("name") or "Por definir"
        partidos.append({"hora": dt_cl.strftime("%H:%M"), "local": home, "visita": away})
    return partidos, None


def _wc_from_thesportsdb(today_cl) -> tuple[list[dict[str, str]], str | None]:
    """Respaldo: TheSportsDB (sin key, plan libre 'key=3'). World Cup league id 4429."""
    headers = {"User-Agent": "agente-noticias/1.0", "Accept": "application/json"}
    # Consultamos el día CL y el día UTC siguiente, porque los partidos vienen en hora local del evento.
    fechas = {today_cl.isoformat(), (today_cl + timedelta(days=1)).isoformat()}
    partidos: list[dict[str, str]] = []
    last_err = None
    for fecha in sorted(fechas):
        try:
            resp = requests.get(
                "https://www.thesportsdb.com/api/v1/json/3/eventsday.php",
                params={"d": fecha, "l": "FIFA World Cup"},
                headers=headers,
                timeout=25,
            )
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception as e:
            last_err = e
            continue
        for ev in (data.get("events") or []):
            ts = ev.get("strTimestamp")  # ISO UTC, ej "2026-06-15T19:00:00"
            dt_cl = None
            if ts:
                try:
                    dt_cl = datetime.fromisoformat(ts.replace("Z", "")).replace(tzinfo=timezone.utc).astimezone(SANTIAGO_TZ)
                except Exception:
                    dt_cl = None
            if dt_cl is None:
                continue
            if dt_cl.date() != today_cl:
                continue
            home = ev.get("strHomeTeam") or "Por definir"
            away = ev.get("strAwayTeam") or "Por definir"
            partidos.append({"hora": dt_cl.strftime("%H:%M"), "local": home, "visita": away})
    if not partidos and last_err is not None:
        return [], f"TheSportsDB: {last_err}"
    # Dedupe por (hora, local, visita)
    vistos = set()
    unicos = []
    for p in partidos:
        clave = (p["hora"], p["local"], p["visita"])
        if clave not in vistos:
            vistos.add(clave)
            unicos.append(p)
    return unicos, None


def fetch_worldcup_matches_today() -> tuple[list[dict[str, str]], str | None]:
    """Partidos del Mundial de HOY con hora de Chile. Solo durante el torneo.

    Intenta football-data.org (con reintento) y, si falla o no trae partidos,
    cae automáticamente a TheSportsDB (sin key). Defensivo: nunca rompe el briefing.
    """
    today_cl = datetime.now(SANTIAGO_TZ).date()
    if not (WORLDCUP_START <= today_cl <= WORLDCUP_END):
        return [], None  # fuera del Mundial

    d_from = (today_cl - timedelta(days=1)).isoformat()
    d_to = (today_cl + timedelta(days=1)).isoformat()

    notas: list[str] = []
    partidos, err = _wc_from_football_data(today_cl, d_from, d_to)
    if err:
        notas.append(err)
    if partidos:
        partidos.sort(key=lambda p: p["hora"])
        return partidos, (" | ".join(notas) or None)

    # Fallback automático a TheSportsDB
    partidos, err2 = _wc_from_thesportsdb(today_cl)
    if err2:
        notas.append(err2)
    partidos.sort(key=lambda p: p["hora"])
    return partidos, (" | ".join(notas) or None)


def build_worldcup_html(partidos: list[dict[str, str]]) -> str:
    """Bloque compacto de partidos del día (hora de Chile). Vacío si no hay partidos."""
    if not partidos:
        return ""
    filas = "".join(
        f'<tr><td style="padding:2px 14px 2px 0;color:#6b7280;white-space:nowrap;">{html_module.escape(p["hora"])}</td>'
        f'<td style="padding:2px 0;">{html_module.escape(p["local"])} <span style="color:#9ca3af;">vs</span> {html_module.escape(p["visita"])}</td></tr>'
        for p in partidos
    )
    return (
        '<div style="margin:14px 0 0 0;font-size:13.5px;">'
        '<p style="margin:0 0 6px 0;font-weight:600;">⚽ Mundial — partidos de hoy (hora Chile)</p>'
        f'<table style="border-collapse:collapse;">{filas}</table>'
        "</div>"
    )


def compose_email_document(
    price_block: str,
    news_block: str,
    news_errors: list[str],
    matches_block: str = "",
) -> str:
    """Documento HTML completo: tabla de precios (código) + bloque de noticias."""
    hr_block = '<hr style="margin:28px 0;border:none;border-top:1px solid #e5e7eb;" />' if price_block.strip() else ""
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
{matches_block}
{hr_block}
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
        destinos = [d.strip() for d in destino.split(",") if d.strip()]
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = gmail_user
        msg["To"] = ", ".join(destinos)

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
            server.sendmail(gmail_user, destinos, msg.as_string())
        return True, None
    except Exception as e:
        return False, f"SMTP: {e}\n{traceback.format_exc()}"


def run() -> int:
    try:
        env = load_env()
    except Exception as e:
        print(f"Error al cargar .env: {e}", file=sys.stderr)
        return 1

    mode = get_briefing_mode()
    print(f"Modo: {mode}")

    if mode != "vespertino":
        print("Obteniendo precios…")
        prices, price_errors = fetch_prices()
    else:
        prices, price_errors = [], []
    if price_errors:
        for err in price_errors:
            print(f"  [precios] {err}", file=sys.stderr)
    print(f"  OK: {len(prices)} activos con datos.")

    lookback_h = get_news_lookback_hours()
    print(f"Obteniendo noticias (ventana últimas {lookback_h} h)…")
    news, news_errors, news_meta = fetch_news_24h(env["NEWS_API_KEY"], lookback_h)
    trump_items, trump_err = fetch_trump_news(env["NEWS_API_KEY"], lookback_h)
    if trump_err:
        print(f"  [Trump news] {trump_err}", file=sys.stderr)
    news_meta["trump_fetch"] = {"n": len(trump_items), "error": trump_err}
    apollo_items, apollo_err = fetch_apollo_daily_spark(lookback_h)
    news_meta["apollo_daily_spark"] = {"n": len(apollo_items), "error": apollo_err}
    if apollo_err:
        news_errors = list(news_errors)
        news_errors.append(apollo_err)
    if mode != "vespertino":
        jpm_items, jpm_errs, jpm_meta = fetch_jpm_am_watch_updates()
    else:
        jpm_items, jpm_errs, jpm_meta = [], [], {"disabled": True}
    news_meta["jpm_am_watch"] = jpm_meta
    if jpm_errs:
        news_errors = list(news_errors)
        news_errors.extend(jpm_errs)
    print("Obteniendo fuentes Flash Report (Chile, watchlist FO, celulosa, históricos)…")
    flash_items, flash_errs, flash_meta = fetch_flash_report_sources()
    news_meta["flash_report_sources"] = flash_meta
    if flash_errs:
        news_errors = list(news_errors)
        news_errors.extend(flash_errs)
        for err in flash_errs:
            print(f"  [flash sources] {err}", file=sys.stderr)
    print(f"  OK: {len(flash_items)} titulares Flash Report ({json.dumps(flash_meta, ensure_ascii=False)}).")
    print("Obteniendo noticias internacionales premium (Reuters, Bloomberg, CNBC, WSJ, MarketWatch, Yahoo, FT)…")
    premium_items, premium_errs, premium_meta = fetch_premium_intl_news()
    news_meta["premium_intl"] = premium_meta
    if premium_errs:
        news_errors = list(news_errors)
        news_errors.extend(premium_errs)
        for err in premium_errs:
            print(f"  [premium intl] {err}", file=sys.stderr)
    print(f"  OK: {len(premium_items)} titulares premium ({json.dumps(premium_meta, ensure_ascii=False)}).")
    # trump_items ya tienen trump_priority=True; van primero pero dedupe los quita si duplicados
    # ORDEN IMPORTA: el payload se recorta (abajo), así que las fuentes escasas/clave
    # (chilenas, watchlist, celulosa, históricos) van PRIMERO para no quedar truncadas
    # detrás de las ~150 internacionales premium.
    news = flash_items + apollo_items + trump_items + jpm_items + premium_items + news
    if news_errors:
        for err in news_errors:
            print(f"  [noticias] {err}", file=sys.stderr)
    print(
        f"  OK: {len(news)} titulares tras filtro {lookback_h} h "
        f"(crudos API: {news_meta.get('recibidos_crudos', 0)}; Trump: {len(trump_items)}; "
        f"Apollo Daily Spark: {len(apollo_items)}; JPM AM: {len(jpm_items)} nuevos)."
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

    price_html = build_prices_table_html(prices, price_errors) if mode != "vespertino" else ""

    # Partidos del Mundial (solo durante el torneo; vacío el resto del año).
    matches_html = ""
    if mode != "vespertino":
        partidos, wc_err = fetch_worldcup_matches_today()
        if wc_err:
            print(f"  [mundial] {wc_err}", file=sys.stderr)
        if partidos:
            print(f"  Mundial: {len(partidos)} partidos hoy.")
            matches_html = build_worldcup_html(partidos)

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
            news_html = normalized
        else:
            print(
                "Claude devolvió contenido no HTML (p. ej. JSON); usando listado de titulares.",
                file=sys.stderr,
            )
            news_html = build_news_fallback_html_sections(news)

    subject_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if mode == "vespertino":
        subject = f"Resumen vespertino — {subject_date}"
    else:
        subject = f"⚡ Flash Report — {subject_date}"
    html = compose_email_document(price_html, news_html, news_errors, matches_html)

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
