# Agente de Noticias Diario

## Proyecto

**Agente de Noticias Diario**

## Qué hace

Script Python que cada mañana a las 8am (Chile) obtiene precios de activos financieros y noticias de economía y mercados, genera un resumen en español con Claude AI y lo envía por email.

## Stack

- **Python 3.11**
- **yfinance** — precios de activos
- **NewsAPI** — noticias
- **Claude API / claude-haiku-4-5** — resumen con IA
- **Gmail SMTP** — envío de email
- **GitHub Actions** — automatización diaria

## Activos monitoreados

Futuros S&P 500, FTSE 100, Nikkei 225, Hang Seng, Shanghai, Oro, Petróleo WTI, Petróleo Brent, Cobre, Bono EEUU 2A, Bono EEUU 10A, Bitcoin, ETH/USD, EUR/USD, USD/CLP

## Categorías de noticias

Economía, Internacional, Cripto, Corporativo, Mercados

En **`daily_briefing.py`** y **`alert_agent.py`**, el prompt a Claude incluye exclusiones explícitas: no se usan titulares de **apuestas deportivas / sports betting / gambling** sin vínculo con inversión, ni noticias **sin relación directa** con mercados financieros, economía macro, resultados corporativos relevantes o cripto en contexto de mercado.

## Variables de entorno necesarias (`.env`)

`ANTHROPIC_API_KEY`, `NEWS_API_KEY`, `GMAIL_USER`, `GMAIL_PASSWORD`, `EMAIL_DESTINO`

## Automatización

GitHub Actions con cron `0 11 * * *` (11:00 UTC ≈ 8:00 Chile). Los lunes busca noticias de **72 horas** para cubrir el fin de semana.

## Archivos principales

- **`daily_briefing.py`** — script principal
- **`alert_agent.py`** — alertas de portafolio (ver abajo)
- **`swing_signals.py`** — señales swing S&P 500 (ver abajo)
- **`.github/workflows/daily_briefing.yml`** — briefing diario
- **`.github/workflows/alert_agent.yml`** — alertas cada 4 h
- **`.github/workflows/swing_signals.yml`** — swing lun–vie
- **`.env`** — credenciales locales (no se sube a GitHub)

## `alert_agent.py`

Monitorea noticias relevantes para un portafolio de **36 acciones** (watchlist personal).

- **Fuentes:** NewsAPI + RSS de Reuters, CNBC y Reserva Federal
- **Frecuencia:** cada **4 horas** vía GitHub Actions
- Solo envía email si **Claude** determina que hay noticias relevantes
- **Criterios:** movimientos >2%, resultados corporativos, decisiones Fed, geopolítica, crypto

## `swing_signals.py`

Analiza el **S&P 500 completo** buscando oportunidades de swing trading.

- **Datos:** Yahoo Finance (**6 meses** de histórico diario)
- **Señales:** RSI, medias móviles 20/50, volumen relativo, ATR
- **Filtros:** retorno esperado **5–10%**, ratio riesgo/beneficio mínimo **1:2**, volumen relativo **>1,5**
- **Salida:** máximo **3** señales de alta convicción (long o short) con entrada, stop loss, take profit y precio máximo/mínimo de entrada (según lado)
- **Frecuencia:** lunes a viernes a las **12:00 UTC** (**8:30am Chile** aprox.) vía GitHub Actions
- Si no hay señales de calidad, **no envía email**
