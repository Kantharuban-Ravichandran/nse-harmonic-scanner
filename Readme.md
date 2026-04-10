# NSE Harmonic Scanner — Setup

## 1. Repository Structure

```
your-repo/
├── harmonic_scanner.py
├── requirements.txt
└── .github/
    └── workflows/
        └── nse_scanner.yml
```

## 2. GitHub Secrets

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | From @BotFather |
| `TELEGRAM_CHAT_ID` | Your chat/channel ID (use @userinfobot) |
| `GOOGLE_SHEET_ID` | From the Google Sheet URL |
| `GOOGLE_CREDS_JSON` | Full JSON of service account key (minified, no newlines) |
| `EMAIL_FROM` | Gmail address used to send |
| `EMAIL_TO` | Recipient email |
| `EMAIL_PASSWORD` | Gmail App Password (not your login password) |
| `EMAIL_SMTP_HOST` | `smtp.gmail.com` (default) |
| `EMAIL_SMTP_PORT` | `587` (default) |

## 3. Telegram Bot Setup

```
1. Open Telegram → search @BotFather
2. /newbot → give name & username
3. Copy the token → TELEGRAM_BOT_TOKEN
4. Start a chat with your bot OR create a channel
5. Send a message, then visit:
   https://api.telegram.org/bot<TOKEN>/getUpdates
6. Copy the chat.id value → TELEGRAM_CHAT_ID
```

## 4. Google Sheets Service Account

```
1. console.cloud.google.com → New Project
2. Enable: Google Sheets API, Google Drive API
3. IAM → Service Accounts → Create → Download JSON key
4. Open your Google Sheet → Share → paste service account email (Editor)
5. Minify the JSON key (remove all newlines) → paste as GOOGLE_CREDS_JSON
6. Copy the Sheet ID from its URL → GOOGLE_SHEET_ID
```

## 5. Gmail App Password

```
1. myaccount.google.com → Security → 2-Step Verification (enable)
2. Security → App passwords → Select app: Mail → Generate
3. Copy the 16-char password → EMAIL_PASSWORD
```

## 6. Local Testing

```bash
pip install -r requirements.txt

export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
export GOOGLE_SHEET_ID=...
export GOOGLE_CREDS_JSON='{"type":"service_account",...}'
export EMAIL_FROM=you@gmail.com
export EMAIL_TO=you@gmail.com
export EMAIL_PASSWORD=...

python harmonic_scanner.py
```

## 7. Schedule

The workflow runs **Monday–Friday at 5:00 PM IST (11:30 UTC)**.
To trigger manually: GitHub → Actions → NSE Harmonic Scanner → Run workflow.

## 8. Tuning Parameters (env vars or hardcode)

| Variable | Default | Description |
|---|---|---|
| `ZIGZAG_DEPTH` | `12` | ZigZag pivot sensitivity |
| `PRICE_PERIOD` | `1y` | yfinance lookback |
| `BATCH_SIZE` | `50` | Symbols per download batch |
| `MAX_WORKERS` | `8` | Parallel download threads |
| `FIB_TOLERANCE` | `0.05` | ±5% Fibonacci ratio tolerance |

## 9. Patterns Detected

Gartley · Bat · Butterfly · Crab · DeepCrab · Shark · Cypher · ABCD
