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
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
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

GNEWS_QUERIES_NACIONAL: tuple[tuple[str, str], ...] = (
    # (query, etiqueta para logs)
    ("economía OR mercados OR IPSA OR dólar Chile when:1d", "chile_economia"),
    ("(Senado OR gobierno OR Hacienda OR reforma) Chile when:1d", "chile_politica"),
    ("(banco OR banca OR seguros de vida OR eléctrica OR energía) Chile when:1d", "chile_sectores"),
)

GNEWS_QUERIES_WATCHLIST: tuple[tuple[str, str], ...] = (
    ('"CMPC" OR "Colbún" OR "Banco Bice" OR "Bicecorp" OR "Banco Security" OR "Bice Vida" OR "Arauco" when:2d', "fo_watchlist"),
)

GNEWS_QUERIES_CELULOSA: tuple[tuple[str, str], ...] = (
    ('(celulosa OR pulp) (Suzano OR Klabin OR UPM OR "Stora Enso" OR precio OR demand OR China OR Brasil OR Europe) when:2d', "celulosa_global"),
)

GNEWS_QUERIES_HISTORICO_7D: tuple[tuple[str, str], ...] = (
    ("(IPC OR Imacec OR PIB OR \"Banco Central\" OR reforma) Chile when:7d", "hist_chile"),
    ('("CPI" OR "Federal Reserve" OR "jobs report" OR GDP) "United States" when:7d', "hist_eeuu"),
)

# Estado del Market Trending (para detectar temas 🔴 en retirada vs días previos).
TRENDING_STATE_FILENAME = "trending_state.json"

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

            es_tasa = symbol in YIELD_SYMBOLS_PCT
            if es_tasa:
                # Para tasas la variación se expresa en puntos base, no en %.
                pct = (last - prev) * 100.0
            elif prev and prev != 0:
                pct = (last - prev) / prev * 100.0
            else:
                pct = 0.0

            rows.append(
                {
                    "activo": label,
                    "ticker": symbol,
                    "precio": round(last, 6),
                    "variacion_pct": round(pct, 4),
                    "es_tasa": es_tasa,
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
    """Bloque monoespaciado de indicadores, estilo Flash Report (sin tabla HTML)."""
    if not rows:
        return ""
    ancho_activo = max(len(r["activo"]) for r in rows) + 2

    lineas: list[str] = []
    for r in rows:
        pct = float(r["variacion_pct"])
        if r.get("es_tasa"):
            precio = f"{float(r['precio']):.2f}%"
            if abs(pct) < 0.5:
                var_str = "="
            else:
                var_str = f"{'+' if pct > 0 else ''}{pct:.0f} pb"
        else:
            precio = _format_price_display(float(r["precio"]))
            var_str = f"{'+' if pct > 0 else ''}{pct:.2f}%"
        lineas.append(
            f"{r['activo']:<{ancho_activo}}|  {precio:>11}  |  {var_str:>8}"
        )

    cuerpo = html_module.escape("\n".join(lineas))
    parts = [
        '<pre style="font-family:Consolas,Menlo,monospace;font-size:13.5px;'
        'background:#f6f8fa;border:1px solid #e5e7eb;border-radius:8px;'
        'padding:14px 16px;overflow-x:auto;line-height:1.7;margin:0;">'
        f"{cuerpo}</pre>"
    ]
    if price_errors:
        parts.append(
            "<p style='margin-top:10px;font-size:13px;color:#92400e;'><strong>Avisos:</strong> "
            + html_module.escape(" | ".join(price_errors))
            + "</p>"
        )
    return "\n".join(parts)

def build_claude_prompt_news_only(
    news: list[dict[str, Any]],
    news_errors: list[str],
    temas_previos: list[str] | None = None,
) -> str:
    """Prompt maestro del Flash Report: la tabla de indicadores la genera el script."""
    payload = {
        "noticias": news[:110],
        "errores_noticias": news_errors,
        "temas_trending_dias_previos": temas_previos or [],
        "fecha_hoy": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
    return f"""Actúa como un analista financiero y político de alto nivel. Con los titulares del JSON (campo "noticias") debes construir un "Flash Report" ejecutivo diario, hiper-scannable, para lectura rápida en pantalla (sin paredes de texto). No inventes noticias: usa solo lo que viene en el JSON.

{json.dumps(payload, ensure_ascii=False, indent=2)}

=== EXCLUSIONES (descarta ANTES de clasificar) ===
- Apuestas deportivas, gambling, lifestyle financiero, consejos de inversión genéricos ("cómo invertir", "guía", "¿deberías comprar X?").
- Opinión de pundits o celebridades sin hecho noticioso concreto; listas/rankings de acciones sin evento; clickbait sin hecho verificable.
- Política partidista sin impacto demostrable en mercados, tasas, reformas económicas o regulación.
- Beneficios estatales y trámites para personas: bonos (Bono por Hijo, bonos invierno, etc.), subsidios, "consulta con tu RUT", fechas de pago, requisitos de postulación, pensiones individuales. Descarta SIEMPRE.
- Noticias de consumo u operativa cotidiana: medios de pago aceptados en transporte o comercios, promociones bancarias a clientes, concursos, lanzamientos de apps de consumo. Descarta.
- En los sectores prioritarios (banca, seguros de vida, energía) solo entran noticias CORPORATIVAS: resultados, fusiones/adquisiciones, inversiones, emisiones de deuda, regulación con impacto en las empresas, nombramientos clave. Lo demás del sector se descarta.
- EXCEPCIÓN ABSOLUTA: nunca descartes una noticia que mencione a {", ".join(FO_WATCHLIST)} — esas se incluyen SIEMPRE, sin importar su tamaño o relevancia aparente.

=== ESTRUCTURA OBLIGATORIA DEL HTML (en este orden exacto) ===

(1) MARKET TRENDING
<h2>📈 Market Trending</h2> seguido de hasta 10 párrafos <p style="margin:0 0 10px 0;">, una sola línea de idea fuerza cada uno (sin "Título: Explicación", sin fuentes, sin links, sin viñetas):
- 🟢 al inicio del <p> para los temas que HOY dominan el flujo noticioso (los detectas por cantidad/peso de titulares en el JSON).
- 🔴 para temas que aparecen en "temas_trending_dias_previos" pero que HOY casi no tienen titulares (en retirada). Incluye 2-3 si existen; si "temas_trending_dias_previos" viene vacío, no inventes 🔴.
Al FINAL de todo el HTML (después de la última sección), agrega en una sola línea el comentario: <!--TRENDING_JSON:["tema1","tema2",...]--> con los temas 🟢 de hoy (máximo 10, strings cortos). Es obligatorio para la memoria del sistema.

(2) BLOQUE INTERNACIONAL
<h2>I. INTERNACIONAL</h2> con tres subsecciones <h3>: Macroeconomía, Precios y Mercados, Política.

(3) BLOQUE NACIONAL
<h2>II. NACIONAL</h2> con las mismas tres subsecciones <h3>. Usa las noticias con "nacional": true. El título es "II. NACIONAL" a secas (sin "Chile").

=== REGLAS DE REDACCIÓN (aplican a todas las noticias) ===
- PROHIBIDO el formato "Título: Explicación" o "Tema: detalle". NUNCA empieces una noticia con una etiqueta en negrita seguida de dos puntos (MAL: "<strong>Banco de Chile:</strong> se posiciona como marca más valiosa"). Redacta la idea fuerza directamente con las palabras clave en negrita DENTRO de la frase (BIEN: "<strong>Banco de Chile</strong> se posiciona como la marca más valiosa del país y acelera su financiamiento automotriz 100% digital").
- ÚNICAS excepciones con prefijo permitido: "Hoy:" / "Hoy reporta:" (agenda), los prefijos geográficos de Macroeconomía Internacional (<strong>EE.UU.:</strong>, <strong>China:</strong>, <strong>Europa:</strong>, <strong>Otros:</strong>) y <strong>Celulosa:</strong>.
- Máximo DOS líneas por noticia (~220 caracteres). Idea fuerza de inmediato.
- Negritas <strong> en palabras clave de identificación rápida (dólar, IPC, Fed, CMPC, cobre…).
- Datos económicos: SIEMPRE métrica exacta + comparación explícita contra expectativas si está disponible ("IPC subió 0,4% MoM vs 0,3% esperado").
- Noticias corporativas: estilo directo con cifras concretas y dato prospectivo. Ejemplo de referencia: "Oracle cae más del 7% en la pre apertura, luego de que el gasto en centros de datos superara las estimaciones. La compañía prevé invertir ~US$70.000 millones en capex el año fiscal que termina en mayo 2027."
- Consolidación: dos noticias del mismo activo/tema → un solo bullet.
- Fuente: SOLO el hipervínculo, sin duplicar el nombre. El <li> termina con <a href="URL_EXACTA_DEL_JSON" style="color:#1d4ed8;">Fuente</a> usando el campo "fuente" como texto del link. Si "url" viene vacía, cierra con <span style="color:#6b7280;font-size:12px;">({{fuente}})</span>.

=== AGENDA DEL DÍA (obligatoria si hay material) ===
- En Macroeconomía (Internacional y Nacional): un bullet inicial "Hoy:" con los datos económicos que se publican hoy según los titulares (ej. "Hoy se conocerá el IPP de mayo en EE.UU. y las solicitudes semanales de desempleo"). Solo la frase, sin explicación.
- Comentarios, decisiones o señales de la Fed y del BCE son IGUAL O MÁS relevantes que los datos: inclúyelos siempre en Macroeconomía Internacional con prioridad alta.
- En Precios y Mercados Internacional: un bullet "Hoy reporta:" con los resultados corporativos del día de las principales empresas del S&P 500 (foco tech), si los titulares los mencionan.

=== PRIORIDAD Y ORDEN ===
Precios y Mercados (Internacional): 1º noticias EE.UU. de impacto sistémico (líderes, Fed, hedge funds relevantes); 2º corporativas/sectoriales EE.UU.; 3º Europa/Asia de alta relevancia (elimina lo genérico); 4º Latinoamérica al final.
Macroeconomía (Internacional): agrupa por procedencia con prefijo en negrita y este orden estricto: <strong>EE.UU.:</strong>, <strong>China:</strong>, <strong>Europa:</strong> (o país), <strong>Otros:</strong>.
CELULOSA GLOBAL: las noticias con "celulosa_global": true (producción Brasil, demanda China/Europa, {", ".join(PULP_GLOBAL_WATCHLIST)}) van en Internacional > Precios y Mercados con un bullet que empiece con <strong>Celulosa:</strong>. Inclúyelas siempre que existan.
NACIONAL: las noticias que mencionen a {", ".join(FO_WATCHLIST)} van PRIMERO dentro de su subsección. Después, prioridad alta para sectores banca, seguros de vida y energía. Clasifica cada noticia en la subsección que le corresponda por contenido (una declaración política de un ejecutivo va en Política; un resultado trimestral en Precios y Mercados).
PRIORIDAD TRUMP ("trump_priority": true): declaraciones/órdenes con impacto en mercados van primero en su subsección internacional.
Apollo Daily Spark y J.P. Morgan AM ("apollo_daily_spark"/"jpm_institutional": true): inclúyelos todos, clasificados donde corresponda.

=== HISTÓRICO ÚLTIMA SEMANA ===
OBLIGATORIO: si en el JSON existen noticias con "historico_7d": true, DEBEN aparecer en el reporte. Al final de CADA subsección que tenga material histórico (noticias con "historico_7d": true o hechos de días previos muy relevantes: IPC, Imacec, PIB, reformas, CPI, jobs report), agrega:
<blockquote style="border-left:3px solid #d1d5db;margin:8px 0 4px 0;padding:6px 12px;background:#f9fafb;"><em>📅 Última semana: [resumen consolidado en cursiva, máximo 3 líneas, con links si hay URL]</em></blockquote>
Cada noticia histórica va en la subsección temática que le corresponde (un IPC pasado en Macroeconomía, no en Política). Si la noticia del día ya quedó vieja (ej. un IPC publicado hace 2+ días), va aquí y no como noticia del día. Si no hay material para una subsección, omite el blockquote.

=== FORMATO DE SALIDA ===
- Devuelve ÚNICAMENTE el fragmento HTML (sin <!DOCTYPE>, <html>, <head>, <body>). Prohibido markdown y ```.
- PROHIBIDO usar <ul>, <ol> o <li> en todo el reporte: cada noticia y cada tema del trending va en su propio <p style="margin:0 0 14px 0;line-height:1.55;">. La separación visual entre temas es el espacio entre párrafos, no viñetas.
- Si una subsección queda vacía: <p><em>Sin titulares destacados.</em></p>.
- Separa los bloques I y II con <hr style="border:none;border-top:1px solid #e5e7eb;margin:20px 0;">.
- No agregues preguntas, menús ni cierres al final del reporte; solo el comentario TRENDING_JSON."""

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
    # trump_items ya tienen trump_priority=True; van primero pero dedupe los quita si duplicados
    news = apollo_items + trump_items + jpm_items + flash_items + news
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

    trending_state = load_trending_state()
    temas_previos = list(trending_state.get("temas") or [])
    print(f"Temas trending previos ({trending_state.get('fecha')}): {temas_previos}")

    prompt = build_claude_prompt_news_only(news, news_errors, temas_previos)
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
            temas_hoy, normalized = extract_trending_topics_from_html(normalized)
            if temas_hoy:
                state_err = save_trending_state(temas_hoy)
                if state_err:
                    print(f"  [trending] {state_err}", file=sys.stderr)
                else:
                    print(f"  Trending de hoy guardado: {temas_hoy}")
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
