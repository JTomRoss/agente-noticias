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

## Variables de entorno necesarias (`.env`)

`ANTHROPIC_API_KEY`, `NEWS_API_KEY`, `GMAIL_USER`, `GMAIL_PASSWORD`, `EMAIL_DESTINO`

## Automatización

GitHub Actions con cron `0 11 * * *` (11:00 UTC ≈ 8:00 Chile). Los lunes busca noticias de **72 horas** para cubrir el fin de semana.

## Archivos principales

- **`daily_briefing.py`** — script principal
- **`.github/workflows/daily_briefing.yml`** — workflow de automatización
- **`.env`** — credenciales locales (no se sube a GitHub)
