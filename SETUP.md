# Arbitrage Scout — Setup Guide

A cross-marketplace price arbitrage system covering eBay, Gumtree, Dubizzle,
Finn.no and Facebook Marketplace across UK, UAE and Norway.

---

## What it does

1. **Search** — enter a product name, hit Search, and it fires Apify scrapers
   across all configured marketplaces simultaneously.
2. **Compare prices** — all results are converted to GBP for a level comparison.
3. **Spot deals** — the Opportunities tab automatically finds where the same
   product is cheap in one country and expensive in another.
4. **Alert you** — set a price threshold; when a listing hits it you get a
   console message, email, and/or Slack/Discord webhook.

---

## Requirements

- Python 3.9+
- An [Apify account](https://console.apify.com) (free tier works for testing;
  paid plan ~$49/mo for serious volume)

---

## Step 1 — Get your Apify API token

1. Sign up at https://console.apify.com
2. Go to **Settings → Integrations → API token**
3. Copy the token starting with `apify_api_...`

---

## Step 2 — Install Python dependencies

```bash
cd arbitrage-system
pip install -r requirements.txt
```

---

## Step 3 — Configure

```bash
cp .env.example .env
```

Open `.env` and set at minimum:

```
APIFY_TOKEN=apify_api_XXXXXXXXXXXXXXXXXXXX
```

Optionally set `ALERT_EMAIL`, `SMTP_*` or `WEBHOOK_URL` for notifications.

---

## Step 4 — Start the API server

```bash
python api.py
```

You should see:
```
Arbitrage Scout API running on http://localhost:5050
```

---

## Step 5 — Open the dashboard

Open `dashboard.html` in your browser (just double-click the file, or drag
it into Chrome/Firefox). The dashboard calls the local API at
`http://localhost:5050`.

---

## Step 6 — Run your first search

1. Type a product (e.g. **"Sony PS5"** or **"iPhone 14 Pro"**)
2. Set max items per market (30 is a good start)
3. Optionally set an alert threshold (e.g. £150 for a PS5)
4. Click **Search All Markets**
5. Watch results populate — sorted by GBP price automatically

---

## CLI Usage (no browser needed)

```bash
# Search all markets
python cli.py search "Sony PS5" --alert 200

# Search only UK + Norway
python cli.py search "Sony PS5" --markets ebay_uk finn_no gumtree_uk

# List stored results
python cli.py list "Sony PS5"

# Show arbitrage opportunities (£50+ margin)
python cli.py arbitrage "Sony PS5" --margin 50

# Price summary by marketplace
python cli.py summary "Sony PS5"
```

---

## Marketplaces Configured

| Key             | Marketplace             | Country | Currency |
|-----------------|-------------------------|---------|----------|
| ebay_uk         | eBay UK                 | UK      | GBP      |
| ebay_uae        | eBay UAE                | UAE     | USD      |
| gumtree_uk      | Gumtree UK              | UK      | GBP      |
| dubizzle_uae    | Dubizzle UAE            | UAE     | AED      |
| finn_no         | Finn.no                 | Norway  | NOK      |
| facebook_uk     | Facebook Marketplace UK | UK      | GBP      |
| facebook_uae    | Facebook Marketplace UAE| UAE     | AED      |
| facebook_norway | Facebook Marketplace NO | Norway  | NOK      |

---

## Actor Troubleshooting

If a marketplace returns 0 results, the Apify actor may have changed.
To fix: open `scraper.py`, find the `ACTORS` dict, and update the `"id"` field
with a working actor from https://apify.com/store.

Search for the marketplace name, pick one with good ratings and recent activity,
then update the input_builder lambda to match its expected input schema.

---

## Alert Setup (Email — Gmail)

1. Enable 2FA on your Google account
2. Generate an App Password: https://myaccount.google.com/apppasswords
3. In `.env`:
   ```
   EMAIL_ENABLED=true
   SMTP_USER=you@gmail.com
   SMTP_PASSWORD=your_16_char_app_password
   ALERT_EMAIL=you@gmail.com
   ```

## Alert Setup (Slack)

1. Create an incoming webhook in your Slack workspace
2. In `.env`: `WEBHOOK_URL=https://hooks.slack.com/services/...`

## Alert Setup (Discord)

1. In your Discord server: channel settings → Integrations → Webhooks
2. Copy the webhook URL
3. In `.env`: `WEBHOOK_URL=https://discord.com/api/webhooks/...`

---

## File Structure

```
arbitrage-system/
├── api.py          — Flask REST API (start this first)
├── scraper.py      — Apify orchestrator + normalisation
├── database.py     — SQLite storage + queries
├── alerts.py       — Email, webhook, console alerts
├── cli.py          — Command-line interface
├── dashboard.html  — Web UI (open in browser)
├── .env.example    — Config template
├── requirements.txt
└── arbitrage.db    — Created automatically on first run
```

---

## Apify Cost Estimates

| Usage level        | Approx monthly cost |
|--------------------|---------------------|
| 5 searches/day     | Free tier           |
| 20–50 searches/day | $49/mo (Personal)   |
| 200+ searches/day  | $199/mo (Scale)     |

Costs depend on which actors you use — community actors often charge per 1,000
records scraped (~$0.50–$2.00 per actor run).
