"""
Analiza el S&P 500, selecciona candidatos con señales técnicas y envía por email hasta 3 ideas de swing
(solo si hay candidatos que pasan filtros estrictos; sin forzar cantidad).
"""

from __future__ import annotations

import html as html_module
import io
import json
import os
import smtplib
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from anthropic import Anthropic
from dotenv import load_dotenv

DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5"
WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
YF_BATCH_SIZE = 45

# Headers de navegador real para Wikipedia (reduce bloqueos 403 / respuestas vacías).
WIKI_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Fallback si Wikipedia no está disponible (50 nombres líquidos del S&P 500, formato Yahoo).
FALLBACK_SP500_LIQUID: tuple[str, ...] = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "BRK-B",
    "LLY",
    "AVGO",
    "JPM",
    "TSLA",
    "UNH",
    "V",
    "XOM",
    "MA",
    "JNJ",
    "PG",
    "HD",
    "COST",
    "MRK",
    "ABBV",
    "CVX",
    "CRM",
    "BAC",
    "NFLX",
    "AMD",
    "PEP",
    "KO",
    "TMO",
    "ORCL",
    "ACN",
    "MCD",
    "CSCO",
    "WMT",
    "ABT",
    "DHR",
    "TXN",
    "NEE",
    "PM",
    "INTC",
    "QCOM",
    "MS",
    "GS",
    "AMGN",
    "UPS",
    "INTU",
    "CAT",
    "IBM",
    "GE",
)

ENTRY_BUFFER = 0.003
TP_MIN_PCT = 5.0
TP_MAX_PCT = 10.0
LIMITE_ENTRADA_PCT = 0.01
MIN_RISK_REWARD = 2.0


def load_env() -> dict[str, str]:
    load_dotenv()
    keys = ["ANTHROPIC_API_KEY", "GMAIL_USER", "GMAIL_PASSWORD", "EMAIL_DESTINO"]
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")
    return {k: os.environ[k] for k in keys}


def wiki_to_yf_symbol(symbol: str) -> str:
    return str(symbol).strip().replace(".", "-")


def fetch_sp500_tickers() -> tuple[list[str], list[str], str]:
    """
    Intenta cargar el S&P 500 desde Wikipedia (requests + pd.read_html).
    Si falla o no hay tickers, usa lista hardcodeada de 50 líquidos.
    Devuelve (tickers, errores, fuente) con fuente en {'Wikipedia', 'fallback_50_liquidos'}.
    """
    errors: list[str] = []

    try:
        r = requests.get(WIKI_SP500_URL, headers=WIKI_BROWSER_HEADERS, timeout=60)
        r.raise_for_status()
        tables = pd.read_html(io.StringIO(r.text))
    except Exception as e:
        errors.append(f"Wikipedia (descarga o read_html): {e}")
        tickers_fb = list(FALLBACK_SP500_LIQUID)
        print(
            f"Tickers cargados: {len(tickers_fb)} (fuente: fallback — 50 más líquidos del S&P 500)",
            flush=True,
        )
        return tickers_fb, errors, "fallback_50_liquidos"

    if not tables:
        errors.append("Wikipedia: no se encontraron tablas")
        tickers_fb = list(FALLBACK_SP500_LIQUID)
        print(
            f"Tickers cargados: {len(tickers_fb)} (fuente: fallback — 50 más líquidos del S&P 500)",
            flush=True,
        )
        return tickers_fb, errors, "fallback_50_liquidos"

    df = tables[0]
    if "Symbol" not in df.columns:
        errors.append(f"Wikipedia: columnas inesperadas {list(df.columns)}")
        tickers_fb = list(FALLBACK_SP500_LIQUID)
        print(
            f"Tickers cargados: {len(tickers_fb)} (fuente: fallback — 50 más líquidos del S&P 500)",
            flush=True,
        )
        return tickers_fb, errors, "fallback_50_liquidos"

    raw = df["Symbol"].astype(str).tolist()
    tickers = [wiki_to_yf_symbol(s) for s in raw if s and str(s).lower() != "nan"]

    if not tickers:
        errors.append("Wikipedia: lista de símbolos vacía tras parsear")
        tickers_fb = list(FALLBACK_SP500_LIQUID)
        print(
            f"Tickers cargados: {len(tickers_fb)} (fuente: fallback — 50 más líquidos del S&P 500)",
            flush=True,
        )
        return tickers_fb, errors, "fallback_50_liquidos"

    print(f"Tickers cargados: {len(tickers)} (fuente: Wikipedia)", flush=True)
    return tickers, errors, "Wikipedia"


def _rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev).abs(), (low - prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def compute_indicators(hist: pd.DataFrame) -> pd.DataFrame | None:
    if hist is None or hist.empty or len(hist) < 55:
        return None
    h = hist.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in h.columns:
            return None
    h = h.dropna(subset=["Close", "Volume"])
    if len(h) < 55:
        return None

    h["MA20"] = h["Close"].rolling(20).mean()
    h["MA50"] = h["Close"].rolling(50).mean()
    h["RSI14"] = _rsi_wilder(h["Close"], 14)
    h["ATR14"] = _atr(h, 14)
    avg_vol_prev = h["Volume"].shift(1).rolling(20).mean()
    h["RelVol"] = h["Volume"] / avg_vol_prev.replace(0, np.nan)
    return h


def last_row_metrics(h: pd.DataFrame) -> dict[str, Any] | None:
    if h is None or len(h) < 1:
        return None
    last = h.iloc[-1]
    rsi = float(last["RSI14"])
    ma20 = float(last["MA20"])
    ma50 = float(last["MA50"])
    close = float(last["Close"])
    atr = float(last["ATR14"])
    rel_vol = float(last["RelVol"])
    if any(np.isnan([rsi, ma20, ma50, close, atr, rel_vol])):
        return None

    return {
        "cierre_referencia": close,
        "rsi": rsi,
        "ma20": ma20,
        "ma50": ma50,
        "atr": atr,
        "rel_vol": rel_vol,
        "sobre_ma50": close > ma50,
    }


def estimate_horizon_days(atr: float, ref_price: float) -> str:
    """Horizonte 3–10 días según volatilidad (ATR% sobre precio de referencia)."""
    if ref_price <= 0 or atr <= 0:
        return "3-10"
    atr_pct = 100.0 * atr / ref_price
    center = max(3, min(10, round(11.0 - atr_pct * 0.85)))
    lo = max(3, int(center) - 2)
    hi = min(10, int(center) + 2)
    if lo > hi:
        lo = hi
    return f"{lo}-{hi}"


def build_trade_candidate(
    ticker: str, direction: str, cierre_prev: float, atr: float
) -> dict[str, Any] | None:
    """
    Entrada = cierre de referencia ±0,3% (simulación primeros ~30 min).
    TP natural = 3×ATR desde entrada; solo válido si el % está entre 5% y 10%.
    Stop = 1,5×ATR; exige reward/risk >= 2.
    """
    direction = direction.lower().strip()
    if direction not in ("long", "short"):
        direction = "long"
    atr_f = float(atr)
    cl = float(cierre_prev)
    if cl <= 0 or atr_f <= 0 or np.isnan(atr_f) or np.isnan(cl):
        return None

    if direction == "long":
        ent = cl * (1.0 + ENTRY_BUFFER)
        stop = ent - 1.5 * atr_f
        tp = ent + 3.0 * atr_f
        reward = tp - ent
        risk = ent - stop
        pct_pot = (reward / ent) * 100.0 if ent else 0.0
        precio_max_entrada = ent * (1.0 + LIMITE_ENTRADA_PCT)
        precio_min_entrada = None
    else:
        ent = cl * (1.0 - ENTRY_BUFFER)
        stop = ent + 1.5 * atr_f
        tp = ent - 3.0 * atr_f
        reward = ent - tp
        risk = stop - ent
        pct_pot = (reward / ent) * 100.0 if ent else 0.0
        precio_max_entrada = None
        precio_min_entrada = ent * (1.0 - LIMITE_ENTRADA_PCT)

    if risk <= 0 or reward <= 0:
        return None
    if reward / risk < MIN_RISK_REWARD - 1e-9:
        return None
    if pct_pot < TP_MIN_PCT - 1e-9 or pct_pot > TP_MAX_PCT + 1e-9:
        return None

    horiz = estimate_horizon_days(atr_f, cl)

    return {
        "ticker": ticker,
        "direccion": direction,
        "precio_entrada": round(ent, 4),
        "precio_maximo_entrada": round(precio_max_entrada, 4) if precio_max_entrada else None,
        "precio_minimo_entrada": round(precio_min_entrada, 4) if precio_min_entrada else None,
        "stop_loss": round(stop, 4),
        "take_profit": round(tp, 4),
        "pct_potencial": round(pct_pot, 2),
        "porcentaje_potencial": round(pct_pot, 2),
        "horizonte_dias": horiz,
    }


def score_long(rsi: float, rel_vol: float) -> float:
    return max(0.0, 45.0 - rsi) * rel_vol


def score_short(rsi: float, rel_vol: float) -> float:
    return max(0.0, rsi - 55.0) * rel_vol


def _download_one_ticker(ticker: str) -> tuple[str, pd.DataFrame | None]:
    need = ["Open", "High", "Low", "Close", "Volume"]
    try:
        df = yf.Ticker(ticker).history(period="6mo", interval="1d", auto_adjust=True)
        if df is None or df.empty:
            return ticker, None
        if not all(c in df.columns for c in need):
            return ticker, None
        return ticker, df[need].copy()
    except Exception:
        return ticker, None


def download_history_batch(tickers: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if not tickers:
        return out
    with ThreadPoolExecutor(max_workers=14) as ex:
        futs = {ex.submit(_download_one_ticker, t): t for t in tickers}
        for fut in as_completed(futs):
            t, df = fut.result()
            if df is not None:
                out[t] = df
    return out


def analyze_universe(tickers: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Devuelve hasta 10 candidatos (mezcla long/short) con niveles precomputados."""
    errors: list[str] = []
    longs: list[dict[str, Any]] = []
    shorts: list[dict[str, Any]] = []

    for i in range(0, len(tickers), YF_BATCH_SIZE):
        batch = tickers[i : i + YF_BATCH_SIZE]
        print(f"  Descargando yfinance {i + 1}-{min(i + YF_BATCH_SIZE, len(tickers))} / {len(tickers)}…")
        batch_data = download_history_batch(batch)

        for t in batch:
            hist = batch_data.get(t)
            if hist is None or hist.empty:
                continue
            h = compute_indicators(hist)
            if h is None:
                continue
            m = last_row_metrics(h)
            if m is None or m["rel_vol"] <= 1.5:
                continue

            rsi, rel_vol = m["rsi"], m["rel_vol"]
            cierre_ref = m["cierre_referencia"]
            atr_v = m["atr"]

            if rsi < 45 and m["sobre_ma50"]:
                lv = build_trade_candidate(t, "long", cierre_ref, atr_v)
                if lv is not None:
                    lv["score"] = score_long(rsi, rel_vol)
                    lv["rsi"] = rsi
                    lv["ma20"] = m["ma20"]
                    lv["ma50"] = m["ma50"]
                    lv["rel_vol"] = rel_vol
                    longs.append(lv)

            if rsi > 55 and not m["sobre_ma50"]:
                sv = build_trade_candidate(t, "short", cierre_ref, atr_v)
                if sv is not None:
                    sv["score"] = score_short(rsi, rel_vol)
                    sv["rsi"] = rsi
                    sv["ma20"] = m["ma20"]
                    sv["ma50"] = m["ma50"]
                    sv["rel_vol"] = rel_vol
                    shorts.append(sv)

    longs.sort(key=lambda x: x["score"], reverse=True)
    shorts.sort(key=lambda x: x["score"], reverse=True)

    picked: list[dict[str, Any]] = []
    for x in longs[:5]:
        picked.append(x)
    for x in shorts[:5]:
        picked.append(x)

    if len(picked) < 10:
        rest = [x for x in longs[5:] + shorts[5:] if x not in picked]
        rest.sort(key=lambda x: x["score"], reverse=True)
        for x in rest:
            if len(picked) >= 10:
                break
            picked.append(x)

    return picked[:10], errors


def build_claude_prompt(candidates: list[dict[str, Any]]) -> str:
    slim = []
    for c in candidates:
        row = {
            "ticker": c["ticker"],
            "direccion": c["direccion"],
            "precio_entrada": c["precio_entrada"],
            "precio_maximo_entrada": c.get("precio_maximo_entrada"),
            "precio_minimo_entrada": c.get("precio_minimo_entrada"),
            "stop_loss": c["stop_loss"],
            "take_profit": c["take_profit"],
            "porcentaje_potencial": c["porcentaje_potencial"],
            "horizonte_dias": c["horizonte_dias"],
            "rsi": round(c["rsi"], 2),
            "ma20": round(c["ma20"], 4),
            "ma50": round(c["ma50"], 4),
            "rel_vol": round(c["rel_vol"], 2),
            "score": round(c["score"], 4),
        }
        slim.append(row)
    return f"""Eres un analista de swing trading exigente. Candidatos del S&P 500 que ya cumplen filtros duros
(TP entre 5% y 10%, stop 1,5×ATR, ratio riesgo/beneficio ≥ 1:2, entrada con buffer ±0,3% sobre el último cierre).

{json.dumps(slim, ensure_ascii=False, indent=2)}

Selecciona como máximo 3 ideas y solo si tienen alta convicción. Puedes devolver 1 o 2 si el resto es mediocre.
Prioriza calidad sobre cantidad. Diversifica sectores cuando tenga sentido.

REGLAS:
- Solo tickers de la lista. Copia los valores numéricos EXACTOS de precio_entrada, precio_maximo_entrada (long) o precio_minimo_entrada (short), stop_loss, take_profit, porcentaje_potencial y horizonte_dias del candidato elegido (no los modifiques).
- En long, precio_minimo_entrada debe ser null. En short, precio_maximo_entrada debe ser null.

Responde ÚNICAMENTE con JSON válido (sin markdown ni ```):
{{
  "seleccion": [
    {{
      "ticker": "AAPL",
      "direccion": "long",
      "precio_entrada": 123.45,
      "precio_maximo_entrada": 124.68,
      "precio_minimo_entrada": null,
      "stop_loss": 120.0,
      "take_profit": 130.0,
      "porcentaje_potencial": 6.2,
      "horizonte_dias": "4-6",
      "motivo": "breve, en español"
    }}
  ]
}}

"seleccion" puede tener 0 a 3 elementos. Si ninguno merece alerta, devuelve "seleccion": []."""


def call_claude(api_key: str, prompt: str) -> tuple[list[dict[str, Any]] | None, str | None]:
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
        return None, "Claude vacío."

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
        return None, f"JSON inválido: {e}\n{text[:1200]}"

    sel = data.get("seleccion")
    if not isinstance(sel, list):
        return None, "Falta clave seleccion[]"
    out = [x for x in sel if isinstance(x, dict)]
    return out, None


def merge_claude_with_candidates(
    seleccion: list[dict[str, Any]], candidates: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    by_key = {(c["ticker"], c["direccion"]): c for c in candidates}
    merged: list[dict[str, Any]] = []
    for s in seleccion[:3]:
        t = str(s.get("ticker", "")).strip().upper().replace(".", "-")
        d = str(s.get("direccion", "long")).lower().strip()
        key = (t, d)
        base = by_key.get(key)
        if base is None:
            for c in candidates:
                if c["ticker"] == t:
                    base = c
                    break
        if base is None:
            continue
        is_long = base["direccion"] == "long"
        merged.append(
            {
                "ticker": t,
                "direccion": base["direccion"],
                "precio_entrada": base["precio_entrada"],
                "precio_maximo_entrada": base.get("precio_maximo_entrada")
                if is_long
                else None,
                "precio_minimo_entrada": base.get("precio_minimo_entrada")
                if not is_long
                else None,
                "stop_loss": base["stop_loss"],
                "take_profit": base["take_profit"],
                "pct_potencial": base["pct_potencial"],
                "porcentaje_potencial": base["porcentaje_potencial"],
                "horizonte_dias": str(s.get("horizonte_dias") or base["horizonte_dias"]),
                "motivo": str(s.get("motivo") or "").strip()
                or "Señal alineada con criterios técnicos del sistema.",
            }
        )
    return merged


def build_email_html(rows: list[dict[str, Any]], wiki_errors: list[str]) -> str:
    alert = ""
    if wiki_errors:
        alert = (
            '<p style="background:#fffbeb;border:1px solid #fcd34d;padding:10px;">'
            + html_module.escape(" | ".join(wiki_errors))
            + "</p>"
        )

    nota_limite = (
        "<p style=\"color:#374151;font-size:13px;margin-bottom:12px;\"><strong>Límite de ejecución:</strong> "
        "long — no ejecutar si la apertura o el precio en la primera media hora supera el "
        "<em>precio máximo de entrada</em>. Short — no ejecutar si el precio cae por debajo del "
        "<em>precio mínimo de entrada</em>. Si se supera el límite, la señal queda invalidada.</p>"
    )

    trs = []
    for r in rows:
        is_long = r["direccion"].lower() == "long"
        color = "#166534" if is_long else "#991b1b"
        side = "LONG" if is_long else "SHORT"
        limite = (
            r.get("precio_maximo_entrada")
            if is_long
            else r.get("precio_minimo_entrada")
        )
        limite_label = "Máx. entrada" if is_long else "Mín. entrada"
        limite_cell = str(limite) if limite is not None else "—"

        trs.append(
            f"<tr>"
            f'<td style="padding:10px;border:1px solid #e5e7eb;font-weight:700;">{html_module.escape(r["ticker"])}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;color:{color};font-weight:700;">{side}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{r["precio_entrada"]}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{html_module.escape(limite_label)}: {html_module.escape(limite_cell)}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{r["stop_loss"]}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{r["take_profit"]}</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{r["pct_potencial"]}%</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;">{html_module.escape(str(r["horizonte_dias"]))} días</td>'
            f'<td style="padding:10px;border:1px solid #e5e7eb;font-size:13px;">{html_module.escape(str(r["motivo"]))}</td>'
            "</tr>"
        )

    table = (
        '<table style="border-collapse:collapse;width:100%;max-width:1100px;font-size:13px;">'
        "<thead><tr style=\"background:#1e3a5f;color:#fff;\">"
        "<th style=\"padding:10px;text-align:left;\">Ticker</th>"
        "<th style=\"padding:10px;\">Lado</th>"
        "<th style=\"padding:10px;\">Entrada</th>"
        "<th style=\"padding:10px;text-align:left;\">Límite entrada</th>"
        "<th style=\"padding:10px;\">Stop</th>"
        "<th style=\"padding:10px;\">Take profit</th>"
        "<th style=\"padding:10px;\">% potencial</th>"
        "<th style=\"padding:10px;\">Horizonte</th>"
        "<th style=\"padding:10px;text-align:left;\">Motivo</th>"
        "</tr></thead><tbody>"
        + "".join(trs)
        + "</tbody></table>"
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8" /></head>
<body style="font-family:Segoe UI,Roboto,sans-serif;line-height:1.5;color:#111;">
<h1 style="font-size:1.2em;">Swing trading — S&amp;P 500</h1>
<p style="color:#4b5563;">Señales con entrada buffer ±0,3%, TP 5–10% (3×ATR), stop 1,5×ATR, ratio ≥1:2. No es asesoría financiera.</p>
{nota_limite}
{alert}
{table}
<p style="font-size:12px;color:#6b7280;margin-top:16px;">{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
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
        msg.attach(MIMEText("Versión HTML del informe de swing.", "plain", "utf-8"))
        p = MIMEText(html_body, "html", "utf-8")
        p.set_charset("utf-8")
        msg.attach(p)
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=180) as server:
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
        print(e, file=sys.stderr)
        return 1

    wiki_errors: list[str] = []
    print("Descargando listado S&P 500 desde Wikipedia…")
    tickers, werr, _ = fetch_sp500_tickers()
    wiki_errors.extend(werr)
    if not tickers:
        print("No hay tickers; abortando.", file=sys.stderr)
        return 1
    print(f"  Tickers: {len(tickers)}")

    print("Analizando universo (puede tardar varios minutos)…")
    try:
        candidates, _ = analyze_universe(tickers)
    except Exception as e:
        print(f"Error en análisis: {e}\n{traceback.format_exc()}", file=sys.stderr)
        return 1

    if not candidates:
        print("Sin candidatos que cumplan todos los filtros; no se envía email.")
        return 0

    print(f"  Candidatos tras filtros (vol, RSI/MA50, TP 5–10%, R/R): {len(candidates)}")

    prompt = build_claude_prompt(candidates)
    print("Consultando a Claude (máx. 3, puede devolver menos o cero)…")
    seleccion, cerr = call_claude(env["ANTHROPIC_API_KEY"], prompt)
    if cerr:
        print(cerr, file=sys.stderr)
        print("Sin respuesta válida de Claude; no se envía email.", file=sys.stderr)
        return 0

    if not seleccion:
        print("Claude no seleccionó señales; no se envía email.")
        return 0

    final_rows = merge_claude_with_candidates(seleccion, candidates)
    if not final_rows:
        print("La selección de Claude no coincidió con candidatos; no se envía email.")
        return 0

    fecha = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"📈 Swing Trading Signals - {fecha}"
    html_body = build_email_html(final_rows, wiki_errors)

    print("Enviando email…")
    ok, smtp_e = send_email_html(
        env["GMAIL_USER"],
        env["GMAIL_PASSWORD"],
        env["EMAIL_DESTINO"],
        subject,
        html_body,
    )
    if not ok:
        print(smtp_e or "SMTP error", file=sys.stderr)
        return 1

    print("Listo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
