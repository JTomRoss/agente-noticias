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
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, urlparse
from difflib import SequenceMatcher
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
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
    ("USDCLP=X", "USD/CLP"),
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

# PORTADAS económico-financieras: una query site: por diario (UN solo dominio cada una,
# para no caer en el problema de site: multi-dominio + when que devolvía casi nada).
# Es la aproximación "Opción B": lo reciente de cada medio, no su portada editorial literal.
# Se marcan portada=true para que el prompt LIDERE el bloque nacional con ellas.
GNEWS_QUERIES_PORTADAS: tuple[tuple[str, str], ...] = (
    ("site:df.cl when:2d", "portada_df"),
    ("(economía OR mercados OR empresas) site:emol.com when:2d", "portada_emol"),
    ("site:economiaynegocios.cl when:2d", "portada_eyn"),
    ("(economía OR mercados OR empresas OR IPSA) site:latercera.com when:2d", "portada_lt"),
    ("(economía OR mercados OR empresas OR IPSA) site:biobiochile.cl when:2d", "portada_biobio"),
)

# Scrape best-effort de las home económicas (extra opt-in: PORTADAS_SCRAPE=1).
# Selectores genéricos; probablemente requieran ajuste tras ver el output real.
PORTADAS_SITES: tuple[tuple[str, str], ...] = (
    ("Diario Financiero", "https://www.df.cl/"),
    ("Economía y Negocios", "https://www.economiaynegocios.cl/"),
    ("Pulso", "https://www.latercera.com/pulso/"),
    ("Emol", "https://www.emol.com/economia/"),
    ("BioBioChile", "https://www.biobiochile.cl/lista/categorias/economia"),
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
    """Parsea una fecha de noticia a datetime UTC tz-aware.

    Soporta dos formatos:
    - ISO 8601 (NewsAPI, Apollo, JPM, ítems generados por el script).
    - RFC 822 (pubDate de RSS, p.ej. "Mon, 15 Jun 2026 10:30:00 GMT").
    Devuelve None si no se puede parsear.
    """
    if not value or not value.strip():
        return None
    s = value.strip()

    dt: datetime | None = None
    # ISO 8601
    iso = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        dt = None
    # RFC 822 (RSS) si ISO falló
    if dt is None:
        try:
            dt = parsedate_to_datetime(s)
        except (TypeError, ValueError, IndexError):
            dt = None
    if dt is None:
        return None

    # Normalizar a UTC tz-aware (fechas naive se asumen UTC).
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Frescura: corte DURO aplicado en código sobre TODO el pool de noticias.
# El prompt ya NO decide qué es "reciente": lo garantiza el código.
# ---------------------------------------------------------------------------
FRESHNESS_DAILY_H = 48      # ítems "del día" (mar-vie: noticia fresca)
FRESHNESS_MONDAY_H = 96     # lunes: jue/vie siguen siendo lo más fresco (sin prensa fin de semana)
FRESHNESS_CARRYOVER_H = 96  # mar-vie: banda 48-96 h → arrastre mid-week (blockquote, no noticia del día)
FRESHNESS_HISTORICO_H = 168  # ítems historico_7d → recuadro "semana pasada" (solo lunes)


def es_lunes_cl(ahora_utc: datetime | None = None) -> bool:
    """True si en hora de Chile hoy es lunes (define modo semanal del correo)."""
    if ahora_utc is None:
        return datetime.now(SANTIAGO_TZ).weekday() == 0
    return ahora_utc.astimezone(SANTIAGO_TZ).weekday() == 0


def filtrar_por_frescura(
    news: list[dict[str, Any]], *, ahora_utc: datetime | None = None
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    """Aplica el corte de frescura y clasifica el arrastre, antes de armar el prompt.

    Lunes (hora de Chile):
    - ``historico_7d``: ventana de 7 días → alimenta el recuadro "semana pasada".
    - Resto: ≤96 h, todo como noticia fresca (jue/vie son lo más reciente tras el finde).
    Martes a viernes:
    - ``historico_7d``: se descarta (el recuadro semanal solo va los lunes).
    - Resto ≤48 h: noticia fresca del día.
    - Resto 48-96 h: se marca ``arrastre=True`` (blockquote de días previos, no del día).
    - Resto >96 h: se descarta.
    Sin fecha parseable: se DESCARTA siempre (garantía en código).

    Devuelve (noticias_filtradas, descartes) con conteos por fuente para los logs.
    """
    ahora = ahora_utc or datetime.now(timezone.utc)
    lunes = es_lunes_cl(ahora)
    cutoff_fresco = ahora - timedelta(hours=FRESHNESS_MONDAY_H if lunes else FRESHNESS_DAILY_H)
    cutoff_carryover = ahora - timedelta(hours=FRESHNESS_CARRYOVER_H)
    cutoff_hist = ahora - timedelta(hours=FRESHNESS_HISTORICO_H)

    kept: list[dict[str, Any]] = []
    drop_viejo: dict[str, int] = {}
    drop_sin_fecha: dict[str, int] = {}

    for it in news:
        dt = _parse_news_datetime((it.get("fecha") or "").strip())
        fuente = (it.get("fuente") or "?").strip() or "?"
        if dt is None:
            drop_sin_fecha[fuente] = drop_sin_fecha.get(fuente, 0) + 1
            continue

        it.pop("arrastre", None)  # limpieza defensiva

        if it.get("historico_7d"):
            # Material de la semana: solo sobrevive los lunes (y dentro de 7 días).
            if lunes and dt >= cutoff_hist:
                kept.append(it)
            else:
                drop_viejo[fuente] = drop_viejo.get(fuente, 0) + 1
            continue

        if dt >= cutoff_fresco:
            kept.append(it)  # noticia fresca del día
        elif not lunes and dt >= cutoff_carryover:
            it["arrastre"] = True  # mar-vie: días previos relevantes
            kept.append(it)
        else:
            drop_viejo[fuente] = drop_viejo.get(fuente, 0) + 1

    return kept, {"viejo": drop_viejo, "sin_fecha": drop_sin_fecha}


# ---------------------------------------------------------------------------
# Confiabilidad de fuentes: whitelist aplicada a los streams temáticos (nacional,
# watchlist, histórico), donde Google News sin site: deja entrar medios dudosos.
# El lado internacional premium ya está curado por construcción (no se toca).
# CALIBRACIÓN: estos nombres deben coincidir con el <source> real del RSS; ajustar
# con el log "Descartes por fuente" tras las primeras corridas en producción.
# ---------------------------------------------------------------------------
FUENTES_CONFIABLES: frozenset[str] = frozenset({
    # Internacional (para histórico EE.UU. y citas premium en bloques temáticos)
    "reuters", "bloomberg", "cnbc", "wall street journal", "wsj", "marketwatch",
    "yahoo finance", "financial times", "coindesk",
    # Chile — prensa económica y general reputada
    "diario financiero", "df", "dfsud", "df mas", "pulso", "la tercera",
    "el mercurio", "emol", "economia y negocios", "la segunda", "estrategia",
    "america economia", "funds society", "biobiochile", "bio bio", "cooperativa",
    "t13", "tele 13", "cnn chile", "el mostrador", "ex-ante", "ex ante",
    "el libero", "la nacion", "diario sustentable",
})

# Sector celulosa: nicho con prensa especializada que el usuario tolera.
# Estos ítems quedan EXENTOS de la whitelist (solo les aplica la blacklist).
FUENTES_CELULOSA_OK: frozenset[str] = frozenset({"lesprom"})

# Bloqueo explícito (nombradas como dudosas): se descartan SIEMPRE, en cualquier bloque.
FUENTES_BLOQUEADAS: frozenset[str] = frozenset({"xataka", "ad hoc news", "adhoc news"})

# Flags de streams temáticos a los que SÍ se aplica la whitelist.
_TOPIC_FLAGS_WHITELIST: tuple[str, ...] = ("nacional", "fo_watchlist", "historico_7d")


def _norm_fuente(s: str | None) -> str:
    """Normaliza un nombre de fuente: minúsculas, sin acentos, espacios colapsados."""
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s.\-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _fuente_en(fuente_norm: str, permitidas_norm: set[str]) -> bool:
    """True si la fuente coincide (exacta o por contención para nombres ≥5 chars)."""
    if not fuente_norm:
        return False
    for w in permitidas_norm:
        if w == fuente_norm:
            return True
        if len(w) >= 5 and (w in fuente_norm or fuente_norm in w):
            return True
    return False


def filtrar_por_fuente(
    news: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    """Descarta fuentes no confiables en los bloques temáticos (y las bloqueadas en todos).

    - Blacklist global: Xataka, AD HOC NEWS → fuera siempre.
    - Whitelist: solo a ítems nacional/watchlist/histórico (donde entra el ruido).
      El internacional premium y los streams curados (JPM, Apollo, Trump) pasan.
    - Celulosa: exenta de whitelist (sector nicho); solo le aplica la blacklist.
    - Kill-switch: FUENTES_FILTRO_OFF=1 desactiva todo el filtro.

    Devuelve (noticias, descartes) con conteos por fuente para los logs.
    """
    if (os.getenv("FUENTES_FILTRO_OFF") or "").strip().lower() in ("1", "true", "yes"):
        return news, {"bloqueada": {}, "no_whitelist": {}}

    confiables = {_norm_fuente(x) for x in FUENTES_CONFIABLES}
    bloqueadas = {_norm_fuente(x) for x in FUENTES_BLOQUEADAS}

    kept: list[dict[str, Any]] = []
    drop_block: dict[str, int] = {}
    drop_nowl: dict[str, int] = {}

    for it in news:
        fuente = (it.get("fuente") or "").strip()
        fn = _norm_fuente(fuente)
        etiqueta = fuente or "?"

        # 1) Blacklist global (incluye celulosa).
        if _fuente_en(fn, bloqueadas):
            drop_block[etiqueta] = drop_block.get(etiqueta, 0) + 1
            continue

        # 2) Whitelist solo en streams temáticos no-celulosa.
        if not it.get("celulosa_global") and any(it.get(f) for f in _TOPIC_FLAGS_WHITELIST):
            if not _fuente_en(fn, confiables):
                drop_nowl[etiqueta] = drop_nowl.get(etiqueta, 0) + 1
                continue

        kept.append(it)

    return kept, {"bloqueada": drop_block, "no_whitelist": drop_nowl}


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


# ---------------------------------------------------------------------------
# Diseño del correo: el CSS vive AQUÍ (copiado de la maqueta morning_brief_final).
# Claude emite markup con CLASES; el script las convierte a estilos inline (Gmail-safe).
# Así el diseño queda bloqueado en código y no deriva con cada redacción de Claude.
# ---------------------------------------------------------------------------
_SANS = "-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"
_SERIF = "Georgia,'Times New Roman',serif"
EMAIL_CSS: dict[str, str] = {
    "turn": "margin:20px 0 0;font-size:14.5px;line-height:1.5;color:#1a1a1a;border-top:1px solid #e3e3e0;padding-top:16px;",
    "turn-b": "font-weight:700;color:#0f3d2e;",
    "brief": "margin:16px 0 0;border-left:3px solid #0f3d2e;background:#eef3f0;padding:14px 18px 16px;border-radius:0 4px 4px 0;",
    "brief-lbl": "font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#0f3d2e;font-weight:700;margin:0 0 8px 0;",
    "brief-ol": "margin:0;padding-left:18px;",
    "brief-li": "margin:0 0 7px 0;font-size:14px;line-height:1.5;",
    "watch": "margin:12px 0 0;border:1px solid #cfcfca;border-top:3px solid #7a4a00;padding:13px 18px 14px;border-radius:0 0 4px 4px;",
    "watch-lbl": "font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#7a4a00;font-weight:700;margin:0 0 8px 0;",
    "watch-ul": "margin:0;padding-left:18px;",
    "watch-li": "font-size:14px;line-height:1.45;margin:0 0 5px 0;",
    "part": f"font-family:{_SERIF};font-size:13px;letter-spacing:.16em;text-transform:uppercase;color:#1a1a1a;margin:34px 0 0 0;padding-bottom:8px;border-bottom:1px solid #1a1a1a;font-weight:700;",
    "sec": "margin:24px 0 0;",
    "eyebrow": "font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#0f3d2e;font-weight:700;margin:0 0 10px 0;",
    "item": "margin:0 0 14px 0;font-size:14.5px;line-height:1.55;color:#1a1a1a;",
    "lead-item": "margin:0 0 14px 0;font-size:15px;line-height:1.55;color:#1a1a1a;",
    "src": "font-size:12.5px;color:#1a56c4;text-decoration:none;",
    "bin": "margin:6px 0 0;padding-left:16px;",
    "bin-bullet": "margin:0 0 9px 0;font-size:13.5px;line-height:1.5;color:#1a1a1a;",
    "prev": "margin:14px 0 2px 0;border-left:2px solid #cfcfca;padding:2px 0 2px 14px;font-size:12.5px;line-height:1.5;color:#5f6368;font-style:italic;",
    "prev-tag": "font-style:normal;font-weight:700;color:#80868b;font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;display:block;margin-bottom:3px;",
}


def inline_email_classes(html: str) -> str:
    """Convierte class="x y" en style="..." según EMAIL_CSS (Gmail-safe).

    Además: <b>/<strong> sin estilo → negrita; <a> sin estilo → color de link.
    Las clases desconocidas se descartan. Un style propio existente se conserva.
    """
    def _repl_class(m: re.Match) -> str:
        clases = m.group(1).split()
        estilo = "".join(EMAIL_CSS.get(c, "") for c in clases)
        return f'style="{estilo}"' if estilo else ""

    out = re.sub(r'class="([^"]*)"', _repl_class, html)
    out = re.sub(r"<(b|strong)>", r'<\1 style="font-weight:700;">', out)
    out = re.sub(
        r'<a (?![^>]*style=)([^>]*?)>',
        r'<a style="color:#1a56c4;text-decoration:none;" \1>',
        out,
    )
    return out


def build_news_fallback_html_sections(news: list[dict[str, Any]]) -> str:
    """Respaldo cuando Claude falla: estructura simple alineada al rediseño (modo degradado)."""
    def _item(n: dict[str, Any]) -> str:
        t = html_module.escape(n.get("titular") or "")
        url = n.get("url") or ""
        src = html_module.escape(n.get("fuente") or "Fuente")
        if url:
            link = html_module.escape(url, quote=True)
            fuente = f' <a class="src" href="{link}">{src}</a>'
        else:
            fuente = f' <span style="color:#80868b;font-size:12px;">({src})</span>'
        return f'<p class="item">{t}{fuente}</p>'

    intl, nac, cel = [], [], []
    for n in news:
        if n.get("celulosa_global"):
            cel.append(n)
        elif n.get("nacional"):
            nac.append(n)
        else:
            intl.append(n)

    parts: list[str] = []
    for titulo, grupo in (("Internacional", intl), ("Nacional · Chile", nac), ("Celulosa", cel)):
        if not grupo:
            continue
        parts.append(f'<div class="part">{html_module.escape(titulo)}</div>')
        parts.append('<div class="sec">')
        parts.extend(_item(n) for n in grupo[:25])
        parts.append("</div>")
    if not parts:
        parts.append('<p class="item"><em>Sin titulares destacados.</em></p>')
    return inline_email_classes("\n".join(parts))


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
    env = {k: os.environ[k] for k in keys}
    # Lista propia del briefing diario (independiente de alertas/swing/vespertino).
    # Si no está definida, cae a EMAIL_DESTINO para no romper el envío.
    env["EMAIL_DESTINO_BRIEFING"] = (
        os.getenv("EMAIL_DESTINO_BRIEFING") or os.environ["EMAIL_DESTINO"]
    ).strip()
    return env


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
    - Para tasas: cambio en pb (Día/MTD/YTD) respecto al cierre previo correspondiente.
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
                # MTD/YTD de tasas = cambio en PUNTOS BASE desde el cierre del mes/año
                # anterior (nivel actual - nivel ancla). last y ref ya están en % (ej. 4.46).
                mtd_val = round((last - ref_mes) * 100.0, 2) if ref_mes is not None else None
                ytd_val = round((last - ref_anio) * 100.0, 2) if ref_anio is not None else None
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
    """Tabla HTML responsiva de indicadores (se ve bien en celular y desktop).

    Reglas de formato:
    - Precios: cobre con 2 decimales; el resto con 1 decimal.
    - Tasas (Treasury 2A/10A): nivel con 2 decimales; cambios (Día/MTD/YTD) en pb.
    - Retornos (Día/MTD/YTD de precios): 1 decimal. Negativos en rojo.
    Estilos en línea conservadores para máxima compatibilidad con Gmail.
    """
    if not rows:
        return ""

    COBRE_TICKERS = {"HG=F"}  # único precio que mantiene 2 decimales

    def _fmt_precio(r) -> str:
        precio = float(r["precio"])
        if r.get("es_tasa"):
            return f"{precio:.2f}%"
        dec = 2 if r.get("ticker") in COBRE_TICKERS else 1
        return f"{precio:,.{dec}f}"

    def _fmt_diaria(r) -> tuple[str, int]:
        """Devuelve (texto, signo) con signo -1/0/+1."""
        pct = float(r["variacion_pct"])
        if r.get("es_tasa"):
            if abs(pct) < 0.5:
                return "=", 0
            return f"{'+' if pct > 0 else ''}{pct:.0f} pb", (1 if pct > 0 else -1)
        if abs(pct) < 0.05:
            return "0,0%".replace(".", ","), 0
        return f"{'+' if pct > 0 else ''}{pct:.1f}%".replace(".", ","), (1 if pct > 0 else -1)

    def _fmt_acum(r, key) -> tuple[str, int]:
        """MTD/YTD: cambio en pb para tasas, variación % 1 decimal para precios."""
        val = r.get(key)
        if val is None:
            return "n/d", 0
        if r.get("es_tasa"):
            pb = float(val)
            if abs(pb) < 0.5:
                return "=", 0
            return f"{'+' if pb > 0 else ''}{pb:.0f} pb", (1 if pb > 0 else -1)
        v = float(val)
        if abs(v) < 0.05:
            return "0,0%", 0
        return f"{'+' if v > 0 else ''}{v:.1f}%".replace(".", ","), (1 if v > 0 else -1)

    def _celda_num(text: str, signo: int) -> str:
        color = "#137333" if signo > 0 else ("#c5221f" if signo < 0 else "#1a1a1a")
        return (
            '<td align="right" style="padding:7px 0;font-size:13.5px;border-bottom:1px solid #e3e3e0;'
            f'color:{color};white-space:nowrap;font-variant-numeric:tabular-nums;">'
            f"{html_module.escape(text)}</td>"
        )

    th_r = ('style="text-align:right;padding:6px 0;font-weight:600;color:#80868b;'
            'font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;border-bottom:1px solid #cfcfca;"')
    th_l = ('style="text-align:left;padding:6px 0;font-weight:600;color:#80868b;'
            'font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;border-bottom:1px solid #cfcfca;"')
    filas = [
        "<tr>"
        f"<th {th_l}>Activo</th><th {th_r}>Precio</th><th {th_r}>Día</th>"
        f"<th {th_r}>MTD</th><th {th_r}>YTD</th>"
        "</tr>"
    ]

    for r in rows:
        dia_str, dia_s = _fmt_diaria(r)
        mtd_str, mtd_s = _fmt_acum(r, "mtd")
        ytd_str, ytd_s = _fmt_acum(r, "ytd")
        activo = html_module.escape(str(r["activo"]))
        precio = html_module.escape(_fmt_precio(r))
        filas.append(
            "<tr>"
            '<td align="left" style="padding:7px 0;font-size:13.5px;color:#1a1a1a;'
            f'font-weight:600;border-bottom:1px solid #e3e3e0;">{activo}</td>'
            '<td align="right" style="padding:7px 0;font-size:13.5px;color:#1a1a1a;'
            f'white-space:nowrap;font-variant-numeric:tabular-nums;border-bottom:1px solid #e3e3e0;">{precio}</td>'
            + _celda_num(dia_str, dia_s)
            + _celda_num(mtd_str, mtd_s)
            + _celda_num(ytd_str, ytd_s)
            + "</tr>"
        )

    tabla = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="width:100%;border-collapse:collapse;margin:18px 0 4px 0;font-family:{_SANS};">'
        + "".join(filas)
        + "</table>"
    )
    return tabla


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


def fetch_portadas_scrape() -> tuple[list[dict[str, Any]], list[str]]:
    """Scrape best-effort de las home económicas (extra opt-in: PORTADAS_SCRAPE=1).

    Genérico, conservador y BLINDADO: cualquier fallo por sitio se registra y se sigue;
    nunca rompe el run. Marca portada=true y scraped=true. El dedupe por similitud de
    título (en run) elimina lo que repita lo que ya trajeron las queries site:.
    NOTA: los selectores son genéricos y probablemente requieran ajuste tras ver el
    output real (este entorno no tiene red para validarlos).
    """
    if (os.getenv("PORTADAS_SCRAPE") or "").strip().lower() not in ("1", "true", "yes"):
        return [], []
    items: list[dict[str, Any]] = []
    errors: list[str] = []
    ahora_iso = datetime.now(timezone.utc).isoformat()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; EcoterraBrief/1.0)"}
    descartar = ("suscrí", "iniciar sesión", "newsletter", "cookies", "podcast",
                 "ver más", "lee también", "regístrate", "menú")
    vistos: set[str] = set()
    for fuente, url in PORTADAS_SITES:
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            page = resp.text
        except Exception as e:
            errors.append(f"Scrape portada {fuente}: {e}")
            continue
        n = 0
        for m in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', page, re.I | re.S):
            href, inner = m.group(1), _strip_tags(m.group(2))
            if not (35 <= len(inner) <= 150):
                continue
            low = inner.lower()
            if any(x in low for x in descartar):
                continue
            clave = low[:60]
            if clave in vistos:
                continue
            vistos.add(clave)
            full = href if href.startswith("http") else urljoin(url, href)
            items.append({
                "titular": inner,
                "url": full,
                "fuente": fuente,
                "fecha": ahora_iso,
                "nacional": True,
                "portada": True,
                "scraped": True,
            })
            n += 1
            if n >= 4:
                break
    return items, errors


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

    grupos: list[tuple[tuple[tuple[str, str], ...], dict[str, Any], str, str]] = [
        (GNEWS_QUERIES_NACIONAL, {"nacional": True}, "es-419", "CL"),
        (GNEWS_QUERIES_PORTADAS, {"nacional": True, "portada": True}, "es-419", "CL"),
        (GNEWS_QUERIES_WATCHLIST, {"nacional": True, "fo_watchlist": True}, "es-419", "CL"),
        (GNEWS_QUERIES_CELULOSA, {"celulosa_global": True}, "en-US", "US"),
    ]
    # El material de "semana pasada" solo se usa los lunes; el resto de la semana
    # no se consulta (ahorra llamadas y evita que ítems de 7 días lleguen al pool).
    if es_lunes_cl():
        grupos.append((GNEWS_QUERIES_HISTORICO_7D, {"historico_7d": True}, "es-419", "CL"))

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

    # Extra opt-in: scrape de las home económicas (best-effort, blindado).
    scrape_items, scrape_errs = fetch_portadas_scrape()
    if scrape_items:
        meta["portada_scrape"] = len(scrape_items)
        all_items.extend(scrape_items)
    errors.extend(scrape_errs)

    return all_items, errors, meta


def build_claude_prompt_news_only(
    news: list[dict[str, Any]],
    news_errors: list[str],
    memoria_previos: list[str] | None = None,
) -> str:
    """Prompt maestro del Flash Report: la tabla de indicadores la genera el script.

    ``memoria_previos`` es la memoria móvil de los últimos correos (paso 1b); si llega
    vacía, "En contexto" infiere el viraje desde el propio pool.
    """
    lunes = es_lunes_cl()
    payload = {
        "noticias": news[:320],
        "errores_noticias": news_errors,
        "fecha_hoy": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "es_lunes": lunes,
        "memoria_briefings": memoria_previos or [],
    }

    # Mar-vie: cajas "Días previos" al cierre de las secciones. Lunes: recuadro semanal al final.
    bloque_arrastre = "" if lunes else """=== DÍAS PREVIOS (cajas de continuidad, regla de hoy) ===
Algunas noticias vienen con "arrastre": true: hechos de hace 2-3 días que siguen siendo relevantes pero NO son del día. Trátalas así:
- Van como una caja "Días previos" (la clase <div class="prev"> indicada arriba) AL CIERRE de la sección narrativa que les corresponde (Internacional, Nacional o Celulosa), no como noticia del día.
- NO-REPETIR: una historia aparece UNA sola vez. Si ya está como noticia fresca, NO la repitas aquí; y si va aquí, no la dupliques como noticia del día.
- Si una sección no tiene ítems con "arrastre": true, omite su caja (no inventes).
- GEOGRAFÍA: si todos los datos de una caja internacional son del mismo país (típicamente EE.UU.), indícalo UNA SOLA VEZ al inicio.
"""

    bloque_semanal = """=== RESUMEN DE LA SEMANA PASADA (HOY ES LUNES — VA AL FINAL DE TODO EL CORREO) ===
Cierra el correo, DESPUÉS de "También, en breve", con un único recuadro (clase "brief") que prioriza las principales noticias de la semana previa (lunes a viernes). Usa SOLO las noticias con "historico_7d": true.
- NO es un consolidado de todo lo de la semana: SELECCIONA y PRIORIZA lo que un inversor institucional debería recordar.
- Topes ESTRICTOS: máximo 5 internacionales y máximo 4 nacionales. UNA sola línea por bullet, lectura LIGHT.
- NO-REPETIR: no incluyas aquí nada que ya hayas puesto como noticia del día más arriba.
- CLASIFICACIÓN: Internacional = ítems con "gnews_label": "hist_eeuu"; Nacional = ítems con "gnews_label": "hist_chile".
- Formato EXACTO (omite el grupo sin material; si no hay nada, no muestres el recuadro):
<div class="brief"><div class="brief-lbl">Resumen de la semana pasada</div>
<div class="eyebrow">Internacional</div>
<ul class="watch-ul"><li class="watch-li">[idea fuerza en una línea] <a class="src" href="URL">Fuente</a></li></ul>
<div class="eyebrow">Nacional</div>
<ul class="watch-ul"><li class="watch-li">[idea fuerza en una línea] <a class="src" href="URL">Fuente</a></li></ul>
</div>
""" if lunes else ""

    return f"""Eres el editor de un briefing matinal de noticias económicas y financieras para un family office chileno sofisticado. Tu trabajo es SELECCIONAR y REDACTAR las noticias más relevantes del día a partir del JSON (campo "noticias"). Eres un editor con criterio, no un copista: decides qué entra y qué no. No inventes hechos: usa solo lo que viene en el JSON.

{json.dumps(payload, ensure_ascii=False, indent=2)}

=== PRINCIPIO RECTOR: NOTICIAS, NO COMENTARIOS ===
Cada ítem DEBE comunicar un HECHO CONCRETO del día: una cifra publicada, una decisión tomada, una operación anunciada, un movimiento de precio, un evento ocurrido. Si una frase no contiene un hecho verificable y solo describe un clima o una tendencia, NO va.
- PROHIBIDO (comentario vacío, esto NO es noticia): "La inflación en EE.UU. y su impacto en las tasas sigue siendo el factor dominante para los mercados de divisas." / "Las tendencias FX muestran que la inflación sigue siendo el driver principal." / "El mercado sigue los movimientos de oro y bonos ante la volatilidad."
- PROHIBIDO TAMBIÉN: columnas de opinión, análisis, editoriales, ensayos y "papers". Si el ítem describe que un medio "analiza", "examina", "reflexiona sobre", "identifica brechas en" o "plantea un debate sobre" un tema —en lugar de reportar un hecho ocurrido ese día— NO va, aunque venga de una fuente seria. Ejemplo de lo que NO debe entrar: "El eslabón débil del gobierno corporativo es analizado en [medio]: el texto identifica brechas en los directorios..." Eso es análisis, no noticia. Solo entra si hay un HECHO nuevo (una empresa nombró un director, un regulador emitió una norma, una compañía reportó un resultado).
- CORRECTO (hecho + dato): "El <strong>IPC</strong> de mayo en EE.UU. subió 0,5% MoM, sobre el 0,3% esperado, por el alza de energía." / "El <strong>S&P 500</strong> cerró -1,0% tras el dato de inflación, su mayor caída en tres semanas."
La redacción es editorial y fluida (no pegar el titular crudo), pero SIEMPRE anclada a un hecho con su cifra.

=== SELECCIÓN Y CALIDAD DE FUENTES ===
- INTERNACIONAL: usa EXCLUSIVAMENTE noticias con "intl_premium": true (Reuters, Bloomberg, CNBC, WSJ, MarketWatch, Yahoo Finance, Financial Times). JAMÁS uses una fuente chilena para una noticia internacional. ÚNICA excepción: el bloque de Celulosa usa las noticias con "celulosa_global": true.
- NACIONAL: usa las noticias con "nacional": true (portadas económico-financieras de la prensa chilena).
- Descarta cualquier ítem cuyo hecho no entiendas o que no aporte información real.

=== EXCLUSIONES (descarta ANTES de redactar) ===
- Apuestas deportivas, deportes, farándula, entretenimiento, cultura pop, lifestyle.
- Consejos de inversión genéricos ("cómo invertir", "guía", "¿deberías comprar X?"), listas/rankings sin evento, clickbait.
- Opinión de pundits sin hecho concreto; política partidista sin impacto en mercados/tasas/reformas/regulación.
- Beneficios estatales y trámites para personas (bonos, subsidios, "consulta con tu RUT", fechas de pago, pensiones individuales). Descarta SIEMPRE.
- Consumo y operativa cotidiana (medios de pago en transporte/comercio, promociones a clientes, concursos, apps de consumo).
- En sectores prioritarios (banca, seguros de vida, energía) solo entra lo CORPORATIVO: resultados, M&A, inversiones, emisiones de deuda, regulación con impacto en empresas, nombramientos clave.
- WATCHLIST (family office): si hay un HECHO CONCRETO y reciente sobre {", ".join(FO_WATCHLIST)} (resultado, operación, regulación, movimiento), inclúyelo SIEMPRE y ponlo primero en su sección. PERO si NO hay noticia real de esas entidades en el JSON, NO inventes, NO fuerces un comentario ni rellenes con generalidades ("CMPC sigue atenta al mercado…"): simplemente NO menciones la entidad ese día. La regla es "no descartar un hecho real", no "mencionar la entidad sí o sí".

=== JERARQUÍA Y SÍNTESIS (lo más importante de este correo) ===
El correo se lee POR CAPAS: quien tiene 30 segundos saca lo esencial sin bajar; quien tiene 10 minutos profundiza. Tu trabajo es CONSTRUIR el big picture, no salpicar noticias sueltas del mismo peso.
- AGRUPA POR NARRATIVA, no por taxonomía. Noticias con un MISMO motor van JUNTAS en un solo párrafo, no como ítems separados. Ej: si el dólar fuerte hizo caer oro, cobre, BTC y peso → UNA frase, no cuatro.
- JERARQUIZA Y RECORTA: "interesante" ≠ "importante". Test para cada noticia: ¿esto cambia cómo un family office piensa sobre el mundo o el portafolio? Si no, va a "También, en breve" (el bin) o se elimina. No borres cobertura: COMPRÍMELA en el bin.
- Lo regulatorio o de mercado que toca el portafolio NUNCA se sepulta (ej. investigación de la SEC a fondos de private equity = core, porque la oficina invierte en PE/secundarios).
- Objetivo: ~18-22 ítems desarrollados fuera del bin, NO ~40.

=== ESTRUCTURA DEL HTML — USA EXACTAMENTE ESTAS CLASES (el sistema les aplica el estilo) ===
Genera SOLO de "En contexto" hacia abajo (la tabla de activos y los partidos del Mundial los inserta el sistema, no los generes).
NO escribas atributos style="..."; usa SOLO las clases indicadas. Para la negrita usa <b>. Fuentes: <a class="src" href="URL">Fuente</a>; varias se separan con " · ".

(1) EN CONTEXTO — una sola frase de continuidad con el reporte de ayer (qué cambió o dónde se movió el foco):
<p class="turn"><b class="turn-b">En contexto —</b> [una frase].</p>
- Si "memoria_briefings" trae contenido, úsalo para el contraste con días previos; si está vacío, infiere el viraje desde las noticias frescas vs. las "arrastre".
- CRÍTICO: describe solo un cambio REAL. En día plano, dilo explícito ("sin grandes cambios, sigue mandando la Fed"); NUNCA inventes una rotación.

(2) EL DÍA EN TRES LÍNEAS — EXACTAMENTE 3 viñetas, cada una sintetiza un eje del día (resumen ejecutivo):
<div class="brief"><div class="brief-lbl">El día en tres líneas</div>
<ol class="brief-ol">
<li class="brief-li"><b>[idea fuerza]</b> [resto].</li>
</ol></div>

(3) QUÉ MIRAR HOY — MÁXIMO 3 bullets de eventos/datos del DÍA EN CURSO (reportes/datos de hoy), extraídos de las menciones del JSON. Si no hay, pon menos u omite la caja:
<div class="watch"><div class="watch-lbl">Qué mirar hoy</div>
<ul class="watch-ul">
<li class="watch-li">[evento de hoy].</li>
</ul></div>

(4) LA HISTORIA DEL DÍA — bloque narrativo, MÁXIMO 3 párrafos; une en un mismo párrafo las noticias del mismo motor; lo que es solo "color" baja al bin:
<div class="part">La historia del día</div>
<div class="sec"><p class="lead-item"><b>[qué pasó]</b> [desarrollo]. <a class="src" href="URL">Fuente</a></p> ... (hasta 3)</div>

(5) INTERNACIONAL — agrupa en 1-3 eyebrows temáticos que TÚ eliges según la narrativa del día (NO categorías fijas; ej. "Irán, energía y geopolítica", "Corporativo y regulatorio"). Filtra agresivamente. Big tech con peso en índices, prioridad; cripto solo con hecho real:
<div class="part">Internacional</div>
<div class="sec"><div class="eyebrow">[Tema]</div><p class="item"><b>[qué pasó]</b> [desarrollo]. <a class="src" href="URL">Fuente</a></p> ...</div>
(repite <div class="sec"> por cada eyebrow). Cierra con la caja "Días previos" si hay arrastre.

(6) NACIONAL · CHILE — un solo eyebrow "Portadas económico-financieras". Refleja los titulares económico-financieros que abren la prensa nacional (Diario Financiero, Economía y Negocios de El Mercurio, Pulso de La Tercera y principales diarios). LIDERA con las noticias marcadas "portada": true; luego complementa con el resto de "nacional": true. Las menciones a {", ".join(FO_WATCHLIST)} van PRIMERO. Cierra con "Días previos" si hay arrastre:
<div class="part">Nacional · Chile</div>
<div class="sec"><div class="eyebrow">Portadas económico-financieras</div><p class="item">...</p> ...</div>

(7) CELULOSA — sección CORE (no relleno).
- UMBRAL: solo si hay AL MENOS 3 noticias de celulosa con hecho concreto; si hay 2 o menos, OMITE la sección.
- CONTENCIÓN: la celulosa va ÚNICAMENTE aquí, nunca duplicada. Párrafos autocontenidos (NO viñetas de una frase). Cierra con "Días previos" si hay arrastre:
<div class="part">Celulosa</div>
<div class="sec"><p class="item">...</p> ...</div>

(8) TAMBIÉN, EN BREVE — el bin: one-liners para lo secundario (informativo pero que no cambia la tesis). Permite filtrar sin perder cobertura:
<div class="part">También, en breve</div>
<ul class="bin">
<li class="bin-bullet"><b>[qué pasó]</b> [una línea]. <a class="src" href="URL">Fuente</a></li>
</ul>
{bloque_semanal}
=== CAJA "DÍAS PREVIOS" (cuando corresponda) ===
<div class="prev"><span class="prev-tag">Días previos</span>[idea fuerza, con <a class="src" href="URL">Fuente</a>]</div>

=== REGLA DE NEGRITA (clave del rediseño) ===
La negrita (<b>) es el QUÉ PASÓ, no la entidad. Negrita la IDEA/ACCIÓN (puede ser una frase), para que quien escanee solo las negritas lea las noticias, no un índice de nombres.
- MAL: "<b>Alphabet</b> fue añadida al Dow." → BIEN: "<b>Alphabet entra al Dow</b> en reemplazo de Verizon."
- Un solo <b> por ítem, la idea fuerza al inicio. PROHIBIDO el patrón "<b>Etiqueta:</b> explicación".

=== REDACCIÓN ===
- Texto AUTOCONTENIDO: párrafos breves y explicativos que se entienden sin clickear. NADA de bullets de una frase en las secciones desarrolladas (sí en la capa ejecutiva y en el bin).
- Datos económicos: métrica exacta + comparación vs. lo esperado si está disponible.
- FUENTE: cada noticia cierra con su(s) enlace(s) <a class="src">; el texto del enlace es el nombre de la fuente. Si "url" viene vacía: <span style="color:#80868b;font-size:12px;">(Fuente)</span>.

{bloque_arrastre}=== FORMATO DE SALIDA ===
- Devuelve ÚNICAMENTE el fragmento HTML con las CLASES indicadas (sin <!DOCTYPE>, <html>, <head>, <body>, sin atributos style). Prohibido markdown y bloques de código.
- Si una sección queda sin material, OMÍTELA por completo (no muestres títulos vacíos).
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
    """Bloque de partidos del día (hora de Chile), estilo .fix del mockup. Vacío si no hay."""
    if not partidos:
        return ""
    filas = "".join(
        f'<tr><td style="width:48px;color:#80868b;font-size:13px;padding:2px 0;font-variant-numeric:tabular-nums;">{html_module.escape(p["hora"])}</td>'
        f'<td style="font-size:13px;padding:2px 0;color:#1a1a1a;">{html_module.escape(p["local"])} <span style="color:#80868b;">vs</span> {html_module.escape(p["visita"])}</td></tr>'
        for p in partidos
    )
    return (
        '<div style="margin:18px 0 0;border-top:1px solid #e3e3e0;padding-top:14px;">'
        '<div style="font-size:12px;font-weight:700;color:#5f6368;margin-bottom:6px;">Mundial — partidos de hoy (hora Chile)</div>'
        f'<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">{filas}</table>'
        "</div>"
    )


def compose_email_document(
    price_block: str,
    news_block: str,
    news_errors: list[str],
    matches_block: str = "",
) -> str:
    """Documento HTML email-safe que replica la maqueta (shell 640px, masthead, footer).

    El fragmento de Claude viene con CLASES; aquí se inlinean (Gmail-safe) y se
    insertan los partidos justo antes de "La historia del día".
    """
    _DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    _MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
              "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    hoy = datetime.now(SANTIAGO_TZ)
    fecha_larga = f"{_DIAS[hoy.weekday()]} {hoy.day} de {_MESES[hoy.month - 1]} de {hoy.year}"

    cuerpo = inline_email_classes(news_block)
    if matches_block:
        idx = cuerpo.find("La historia del día")
        if idx == -1:
            idx = cuerpo.find("La historia del dia")
        if idx != -1:
            start = cuerpo.rfind("<", 0, idx)
            start = start if start != -1 else idx
            cuerpo = cuerpo[:start] + matches_block + "\n" + cuerpo[start:]
        else:
            cuerpo = matches_block + "\n" + cuerpo

    mast = (
        '<div style="padding:24px 28px 16px;border-bottom:2px solid #1a1a1a;">'
        '<div style="font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#5f6368;">Ecoterra Report</div>'
        f'<div style="font-family:{_SERIF};font-size:27px;font-weight:700;margin:4px 0 2px;letter-spacing:-.01em;color:#1a1a1a;">Morning Brief</div>'
        f'<div style="font-size:13px;color:#5f6368;">{fecha_larga}</div>'
        "</div>"
    )
    foot = (
        '<div style="padding:24px 28px 32px;color:#80868b;font-size:11.5px;'
        'border-top:1px solid #e3e3e0;margin-top:30px;">'
        "Ecoterra Report · Morning Brief — boletín informativo interno. "
        "Cada noticia enlaza a su fuente original para profundizar."
        "</div>"
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Morning Brief</title>
</head>
<body style="margin:0;padding:0;background:#f3f2ee;color:#1a1a1a;font-family:{_SANS};font-size:15px;line-height:1.55;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f3f2ee;">
<tr><td align="center" style="padding:16px 0;">
<table role="presentation" width="640" cellpadding="0" cellspacing="0" style="max-width:640px;width:100%;background:#ffffff;">
<tr><td>{mast}</td></tr>
<tr><td style="padding:0 28px;">{price_block}{cuerpo}</td></tr>
<tr><td>{foot}</td></tr>
</table>
</td></tr>
</table>
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
        # Separar por coma, punto y coma o saltos de línea, y limpiar cada dirección.
        # Esto evita el error 'folded header contains newline' cuando el secret trae
        # un salto de línea o espacios colados al pegarlo en GitHub.
        import re as _re
        crudos = _re.split(r"[,;\n\r]+", destino or "")
        destinos = [d.strip() for d in crudos if d.strip()]
        if not destinos:
            return False, "Sin destinatarios válidos (EMAIL_DESTINO vacío o mal formado)."
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject.replace("\n", " ").replace("\r", " ").strip()
        msg["From"] = f"Ecoterra Report <{gmail_user.strip()}>"
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


# ---------------------------------------------------------------------------
# Memoria móvil: guarda la síntesis ("El día en tres líneas") de los últimos
# correos para que "En contexto" detecte el cambio de foco vs. días previos.
# El Action commitea el archivo de vuelta (ver workflow). Nunca rompe el envío.
# ---------------------------------------------------------------------------
MEMORIA_PATH = "briefings_memoria.json"
MEMORIA_MAX = 3


def _strip_tags(s: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", s or "")
    txt = html_module.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _extraer_tres_lineas(html_fragment: str) -> list[str]:
    """Extrae las 3 viñetas de la caja 'El día en tres líneas' del HTML generado."""
    if not html_fragment:
        return []
    m = re.search(r"El d[íi]a en tres l[íi]neas.*?<ol[^>]*>(.*?)</ol>", html_fragment, re.I | re.S)
    if not m:
        return []
    lis = re.findall(r"<li[^>]*>(.*?)</li>", m.group(1), re.I | re.S)
    return [t for t in (_strip_tags(li) for li in lis) if t][:3]


def cargar_memoria_briefings() -> list[str]:
    """Lee la memoria y la devuelve como líneas listas para el prompt (reciente primero)."""
    try:
        with open(MEMORIA_PATH, encoding="utf-8") as f:
            data = json.load(f)
        out: list[str] = []
        for b in reversed(data.get("briefings", [])[-MEMORIA_MAX:]):
            lineas = " | ".join(b.get("lineas", []))
            if lineas:
                out.append(f"{b.get('fecha', '?')}: {lineas}")
        return out
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def guardar_memoria_briefing(html_fragment: str, news: list[dict[str, Any]]) -> None:
    """Agrega la síntesis de hoy a la memoria, conservando los últimos MEMORIA_MAX."""
    try:
        lineas = _extraer_tres_lineas(html_fragment)
        if not lineas:  # fallback: titulares líderes del pool enviado
            lineas = [
                (it.get("titular") or "").strip()
                for it in news
                if (it.get("intl_premium") or it.get("nacional")) and (it.get("titular") or "").strip()
            ][:4]
        if not lineas:
            return
        fecha = datetime.now(SANTIAGO_TZ).strftime("%d-%m")
        try:
            with open(MEMORIA_PATH, encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            data = {"version": 1, "briefings": []}
        briefings = [b for b in data.get("briefings", []) if b.get("fecha") != fecha]
        briefings.append({"fecha": fecha, "lineas": lineas})
        data["briefings"] = briefings[-MEMORIA_MAX:]
        with open(MEMORIA_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  Memoria de briefings actualizada ({len(data['briefings'])} guardados).")
    except Exception as e:  # nunca debe tumbar el envío
        print(f"  [memoria] no se pudo guardar: {e}", file=sys.stderr)


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

    # Corte DURO de frescura sobre todo el pool (no se delega al prompt).
    pre_fresh = len(news)
    news, descartes_fresh = filtrar_por_frescura(news)
    n_viejo = sum(descartes_fresh["viejo"].values())
    n_sin_fecha = sum(descartes_fresh["sin_fecha"].values())
    print(
        f"  Tras filtro de frescura: {len(news)} (antes {pre_fresh}; "
        f"descartados {n_viejo} por antigüedad, {n_sin_fecha} sin fecha)."
    )
    if descartes_fresh["viejo"]:
        print(f"    Viejos por fuente: {json.dumps(descartes_fresh['viejo'], ensure_ascii=False)}")
    if descartes_fresh["sin_fecha"]:
        print(f"    Sin fecha por fuente: {json.dumps(descartes_fresh['sin_fecha'], ensure_ascii=False)}")

    # Confiabilidad de fuente (whitelist en bloques temáticos + blacklist global).
    pre_fuente = len(news)
    news, descartes_fuente = filtrar_por_fuente(news)
    n_block = sum(descartes_fuente["bloqueada"].values())
    n_nowl = sum(descartes_fuente["no_whitelist"].values())
    print(
        f"  Tras filtro de fuente: {len(news)} (antes {pre_fuente}; "
        f"bloqueadas {n_block}, fuera de whitelist {n_nowl})."
    )
    if descartes_fuente["bloqueada"]:
        print(f"    Bloqueadas por fuente: {json.dumps(descartes_fuente['bloqueada'], ensure_ascii=False)}")
    if descartes_fuente["no_whitelist"]:
        print(f"    Fuera de whitelist por fuente: {json.dumps(descartes_fuente['no_whitelist'], ensure_ascii=False)}")

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

    memoria_previos = cargar_memoria_briefings()
    if memoria_previos:
        print(f"  Memoria: {len(memoria_previos)} briefing(s) previos para 'En contexto'.")
    prompt = build_claude_prompt_news_only(news, news_errors, memoria_previos)
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

    # Fecha en hora de Chile, formato "15 Jun 2026" con mes en español.
    _MESES_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    _hoy_cl = datetime.now(SANTIAGO_TZ)
    subject_date = f"{_hoy_cl.day:02d} {_MESES_ES[_hoy_cl.month - 1]} {_hoy_cl.year}"
    if mode == "vespertino":
        subject = f"Resumen vespertino | {subject_date}"
    else:
        subject = f"Morning Brief | {subject_date}"
    html = compose_email_document(price_html, news_html, news_errors, matches_html)

    print("Enviando correo (HTML multipart)…")
    ok, smtp_err = send_email_html(
        env["GMAIL_USER"],
        env["GMAIL_PASSWORD"],
        env["EMAIL_DESTINO_BRIEFING"],
        subject,
        html,
    )
    if not ok:
        print(smtp_err or "Error SMTP desconocido", file=sys.stderr)
        return 1

    print("Correo enviado correctamente.")
    guardar_memoria_briefing(news_html, news)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
