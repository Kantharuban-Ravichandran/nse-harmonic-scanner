"""
NSE Harmonic Pattern Scanner
Fully automated EOD scanner for ~2000 NSE stocks
Fixed: Yahoo Finance rate limit handling with session/proxy rotation + sequential fallback
"""

import os
import io
import time
import logging
import json
import smtplib
import warnings
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional
import traceback
import random

import numpy as np
import pandas as pd
import yfinance as yf
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings('ignore')

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('scanner.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '')
GOOGLE_SHEET_ID    = os.getenv('GOOGLE_SHEET_ID', '')
GOOGLE_CREDS_JSON  = os.getenv('GOOGLE_CREDS_JSON', '')
EMAIL_FROM         = os.getenv('EMAIL_FROM', '')
EMAIL_TO           = os.getenv('EMAIL_TO', '')
EMAIL_PASSWORD     = os.getenv('EMAIL_PASSWORD', '')
EMAIL_SMTP_HOST    = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
EMAIL_SMTP_PORT    = int(os.getenv('EMAIL_SMTP_PORT', '587'))

ZIGZAG_DEPTH       = int(os.getenv('ZIGZAG_DEPTH', '12'))
PRICE_PERIOD       = os.getenv('PRICE_PERIOD', '1y')
PRICE_INTERVAL     = os.getenv('PRICE_INTERVAL', '1d')
BATCH_SIZE         = int(os.getenv('BATCH_SIZE', '25'))   # smaller batches = less rate limiting
MAX_WORKERS        = int(os.getenv('MAX_WORKERS', '4'))   # fewer threads = less rate limiting
DOWNLOAD_RETRIES   = int(os.getenv('DOWNLOAD_RETRIES', '3'))
FIB_TOLERANCE      = float(os.getenv('FIB_TOLERANCE', '0.05'))

TV_BASE = "https://www.tradingview.com/chart/?symbol=NSE:"

# ─── User Agents Pool ─────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

def make_session() -> requests.Session:
    """Create a requests session with retry logic and rotating user agent."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return session

# ─── Fibonacci Ratios ─────────────────────────────────────────────────────────
PATTERNS = {
    "Gartley": {
        "XB": (0.618, 0.618), "AC": (0.382, 0.886),
        "BD": (1.272, 1.618), "XD": (0.786, 0.786),
    },
    "Bat": {
        "XB": (0.382, 0.500), "AC": (0.382, 0.886),
        "BD": (1.618, 2.618), "XD": (0.886, 0.886),
    },
    "Butterfly": {
        "XB": (0.786, 0.786), "AC": (0.382, 0.886),
        "BD": (1.618, 2.618), "XD": (1.272, 1.618),
    },
    "Crab": {
        "XB": (0.382, 0.618), "AC": (0.382, 0.886),
        "BD": (2.240, 3.618), "XD": (1.618, 1.618),
    },
    "DeepCrab": {
        "XB": (0.886, 0.886), "AC": (0.382, 0.886),
        "BD": (2.000, 3.618), "XD": (1.618, 1.618),
    },
    "Shark": {
        "XB": (0.446, 0.618), "AC": (1.130, 1.618),
        "BD": (1.618, 2.240), "XD": (0.886, 1.130),
    },
    "Cypher": {
        "XB": (0.382, 0.618), "AC": (1.130, 1.414),
        "BD": (1.272, 2.000), "XD": (0.786, 0.786),
    },
    "ABCD": {
        "AB": (None, None), "BC": (0.382, 0.886),
        "CD": (1.272, 1.618), "AD": (None, None),
    },
}

# ─── Data Classes ─────────────────────────────────────────────────────────────
@dataclass
class HarmonicResult:
    symbol:    str
    pattern:   str
    direction: str
    price:     float
    X: float; A: float; B: float; C: float; D: float
    x_idx: int; a_idx: int; b_idx: int; c_idx: int; d_idx: int
    prz_low:  float = 0.0
    prz_high: float = 0.0
    detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

# ─── NSE Symbol List ──────────────────────────────────────────────────────────
def get_nse_symbols() -> list[str]:
    """Fetch NSE symbols from NSE India CSVs with fallback."""
    urls = [
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftymidsmallcap400list.csv",
    ]
    session = make_session()
    symbols = set()
    for url in urls:
        for attempt in range(3):
            try:
                r = session.get(url, timeout=20)
                if r.status_code == 200:
                    df = pd.read_csv(io.StringIO(r.text))
                    col = next((c for c in df.columns if 'symbol' in c.lower()), None)
                    if col:
                        symbols.update(df[col].dropna().str.strip().tolist())
                    break
            except Exception as e:
                log.warning(f"Symbol fetch attempt {attempt+1} failed: {e}")
                time.sleep(3)

    if not symbols:
        log.warning("Using fallback symbol list.")
        symbols = {
            "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","WIPRO","AXISBANK",
            "LT","BAJFINANCE","BHARTIARTL","ASIANPAINT","MARUTI","TITAN","ULTRACEMCO",
            "SUNPHARMA","TECHM","HCLTECH","POWERGRID","NTPC","ONGC","IOC","BPCL",
            "COALINDIA","GRASIM","INDUSINDBK","KOTAKBANK","NESTLEIND","ADANIENT",
            "BAJAJFINSV","HINDALCO","TATASTEEL","JSWSTEEL","DRREDDY","CIPLA","DIVISLAB",
            "EICHERMOT","TATACONSUM","APOLLOHOSP","BRITANNIA","DABUR","MARICO","PIDILITIND",
            "HAVELLS","SIEMENS","ABB","BOSCHLTD","CUMMINSIND","THERMAX",
        }
    return [f"{s}.NS" for s in sorted(symbols)]


# ─── Core Download Function ───────────────────────────────────────────────────
def download_single(symbol: str, session: requests.Session) -> Optional[pd.DataFrame]:
    """Download single symbol with session. Much more reliable than batch on CI."""
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            # Use yfinance with custom session
            ticker = yf.Ticker(symbol, session=session)
            df = ticker.history(period=PRICE_PERIOD, interval=PRICE_INTERVAL, 
                               auto_adjust=True, timeout=20)
            if df is not None and not df.empty and len(df) >= 30:
                df.index = pd.to_datetime(df.index)
                # Rename columns to standard OHLCV
                df = df[['Open','High','Low','Close','Volume']].dropna()
                if len(df) >= 30:
                    return df
            return None
        except Exception as e:
            err = str(e).lower()
            if 'json' in err or '401' in err or '429' in err or 'rate' in err:
                wait = (attempt + 1) * random.uniform(3, 7)
                log.debug(f"{symbol} attempt {attempt+1} failed: {e}. Waiting {wait:.1f}s")
                time.sleep(wait)
                # Rotate user agent on retry
                session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
            else:
                log.debug(f"{symbol} failed: {e}")
                return None
    return None


def download_batch_with_session(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Download a batch of symbols using a shared session with inter-request delays."""
    session = make_session()
    result = {}
    for i, sym in enumerate(symbols):
        df = download_single(sym, session)
        if df is not None:
            result[sym] = df
        # Rate limit: small random delay between requests
        if i < len(symbols) - 1:
            time.sleep(random.uniform(0.3, 0.8))
    return result


def fetch_all_ohlc(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Parallel batched download with per-batch sessions."""
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    all_data = {}
    log.info(f"Downloading {len(symbols)} symbols in {len(batches)} batches "
             f"(batch_size={BATCH_SIZE}, workers={MAX_WORKERS})...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(download_batch_with_session, b): b for b in batches}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                batch_result = fut.result()
                all_data.update(batch_result)
                log.info(f"  Batch {i}/{len(batches)} done — "
                         f"{len(batch_result)}/{len(futures[fut])} symbols OK "
                         f"(total so far: {len(all_data)})")
            except Exception as e:
                log.error(f"Batch error: {e}")

    log.info(f"Download complete: {len(all_data)}/{len(symbols)} symbols.")
    return all_data


# ─── ZigZag Pivots ────────────────────────────────────────────────────────────
def zigzag_pivots(df: pd.DataFrame, depth: int = ZIGZAG_DEPTH) -> list[tuple]:
    highs = df['High'].values
    lows  = df['Low'].values
    n = len(highs)
    if n < depth * 2:
        return []

    pivots = []
    last_pivot_type = None

    for i in range(depth, n - depth):
        is_high = all(highs[i] >= highs[i-j] for j in range(1, depth+1)) and \
                  all(highs[i] >= highs[i+j] for j in range(1, depth+1))
        is_low  = all(lows[i]  <= lows[i-j]  for j in range(1, depth+1)) and \
                  all(lows[i]  <= lows[i+j]  for j in range(1, depth+1))

        if is_high and last_pivot_type != 'H':
            if pivots and last_pivot_type == 'H' and highs[i] > pivots[-1][1]:
                pivots[-1] = (i, highs[i])
            else:
                pivots.append((i, highs[i]))
            last_pivot_type = 'H'
        elif is_low and last_pivot_type != 'L':
            if pivots and last_pivot_type == 'L' and lows[i] < pivots[-1][1]:
                pivots[-1] = (i, lows[i])
            else:
                pivots.append((i, lows[i]))
            last_pivot_type = 'L'

    return pivots


# ─── Fibonacci Helpers ────────────────────────────────────────────────────────
def fib_ratio(a, b, c):
    if abs(c - a) < 1e-9: return 0.0
    return abs(b - a) / abs(c - a)

def in_range(val, lo, hi, tol=FIB_TOLERANCE):
    if lo is None: return True
    return lo * (1 - tol) <= val <= hi * (1 + tol)

def compute_prz(X, A, B, C, pattern, bullish):
    try:
        r = PATTERNS[pattern]
        xd_lo, xd_hi = r.get("XD", (None, None))
        if xd_lo is None: return (C * 0.99, C * 1.01)
        move = abs(A - X)
        if bullish:
            return (A - move * xd_hi, A - move * xd_lo)
        else:
            return (A + move * xd_lo, A + move * xd_hi)
    except:
        return (C * 0.98, C * 1.02)


# ─── Pattern Matching ─────────────────────────────────────────────────────────
def match_xabcd(pivots: list, pattern: str) -> list:
    results = []
    n = len(pivots)
    if n < 5: return results
    ratios = PATTERNS[pattern]

    for i in range(n - 4):
        X_idx, X = pivots[i]
        A_idx, A = pivots[i+1]
        B_idx, B = pivots[i+2]
        C_idx, C = pivots[i+3]
        D_idx, D = pivots[i+4]

        bullish = A > X

        if pattern == "ABCD":
            bc_r = fib_ratio(X, B, A)
            lo, hi = ratios["BC"]
            if not in_range(bc_r, lo, hi): continue
            cd_ext = abs(C - B) / (abs(A - X) + 1e-9)
            lo2, hi2 = ratios["CD"]
            if not in_range(cd_ext, lo2, hi2): continue
            direction = "Bullish" if X < A else "Bearish"
            results.append(HarmonicResult(
                symbol="", pattern=pattern, direction=direction, price=C,
                X=X, A=A, B=B, C=C, D=C,
                x_idx=X_idx, a_idx=A_idx, b_idx=B_idx, c_idx=C_idx, d_idx=C_idx,
                prz_low=C*0.99, prz_high=C*1.01,
            ))
            continue

        xb_lo, xb_hi = ratios["XB"]
        if not in_range(fib_ratio(X, B, A), xb_lo, xb_hi): continue

        ac_lo, ac_hi = ratios["AC"]
        if not in_range(fib_ratio(A, C, B), ac_lo, ac_hi): continue

        bd_lo, bd_hi = ratios["BD"]
        if not in_range(abs(D - B) / (abs(C - B) + 1e-9), bd_lo, bd_hi): continue

        xd_lo, xd_hi = ratios["XD"]
        if not in_range(fib_ratio(X, D, A), xd_lo, xd_hi): continue

        prz_lo, prz_hi = compute_prz(X, A, B, C, pattern, bullish)
        mn, mx = min(prz_lo, prz_hi), max(prz_lo, prz_hi)
        if not (mn * 0.97 <= D <= mx * 1.03): continue

        results.append(HarmonicResult(
            symbol="", pattern=pattern,
            direction="Bullish" if bullish else "Bearish",
            price=D, X=X, A=A, B=B, C=C, D=D,
            x_idx=X_idx, a_idx=A_idx, b_idx=B_idx, c_idx=C_idx, d_idx=D_idx,
            prz_low=mn, prz_high=mx,
        ))

    return results


def scan_symbol(symbol: str, df: pd.DataFrame) -> list:
    detected = []
    try:
        pivots = zigzag_pivots(df, ZIGZAG_DEPTH)
        if len(pivots) < 5: return detected
        last_bar = len(df) - 1
        for pattern in PATTERNS:
            for m in match_xabcd(pivots, pattern):
                m.symbol = symbol
                if last_bar - m.d_idx <= 3:
                    detected.append(m)
    except Exception as e:
        log.debug(f"scan_symbol {symbol}: {e}")
    return detected


# ─── Charting ─────────────────────────────────────────────────────────────────
STYLE_TV = mpf.make_mpf_style(
    base_mpf_style='nightclouds',
    marketcolors=mpf.make_marketcolors(
        up='#26a69a', down='#ef5350',
        edge='inherit', wick='inherit', volume='in',
    ),
    facecolor='#131722', figcolor='#131722',
    gridcolor='#1e2130', gridstyle='-',
)

def generate_chart(symbol: str, df: pd.DataFrame, result: HarmonicResult) -> bytes:
    tail = 80
    df_plot = df.iloc[-tail:].copy()
    offset = len(df) - tail

    points = {k: (max(0, min(v[0] - offset, tail-1)), v[1]) for k, v in {
        'X': (result.x_idx, result.X), 'A': (result.a_idx, result.A),
        'B': (result.b_idx, result.B), 'C': (result.c_idx, result.C),
        'D': (result.d_idx, result.D),
    }.items()}

    fig, axes = mpf.plot(
        df_plot, type='candle', style=STYLE_TV,
        title=f' {symbol}  |  {result.pattern}  {result.direction}',
        volume=True, returnfig=True, figsize=(12, 7), tight_layout=True,
    )
    ax = axes[0]

    xs = [points[p][0] for p in ['X','A','B','C','D']]
    ys = [points[p][1] for p in ['X','A','B','C','D']]
    ax.plot(xs, ys, 'o-', color='#f0b90b', linewidth=1.5, markersize=4, zorder=5)

    for label, (ix, price) in points.items():
        ax.annotate(label, xy=(ix, price),
                    xytext=(0, 10 if label in ('A','C') else -15),
                    textcoords='offset points', color='#f0b90b',
                    fontsize=9, fontweight='bold', ha='center')

    color = '#26a69a' if result.direction == 'Bullish' else '#ef5350'
    ax.axhspan(result.prz_low, result.prz_high, alpha=0.15, color=color)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=130, bbox_inches='tight',
                facecolor='#131722', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram_photo(image_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    for attempt in range(3):
        try:
            r = requests.post(url, data={
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': caption,
                'parse_mode': 'HTML',
            }, files={'photo': ('chart.png', image_bytes, 'image/png')}, timeout=30)
            if r.status_code == 200:
                return True
            log.warning(f"Telegram error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.warning(f"Telegram attempt {attempt+1}: {e}")
        time.sleep(3)
    return False

def format_telegram_caption(r: HarmonicResult) -> str:
    sym_clean = r.symbol.replace('.NS', '')
    arrow = '🟢' if r.direction == 'Bullish' else '🔴'
    return (
        f"{arrow} <b>{r.pattern}</b> — {r.direction}\n"
        f"📌 <a href='{TV_BASE}{sym_clean}'>{sym_clean}</a>\n"
        f"💰 Price: <b>₹{r.price:.2f}</b>\n"
        f"📊 PRZ: ₹{r.prz_low:.2f} – ₹{r.prz_high:.2f}"
    )


# ─── Google Sheets ────────────────────────────────────────────────────────────
def get_gsheet():
    if not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = sh.worksheet("Alerts")
        except:
            ws = sh.add_worksheet(title="Alerts", rows="1000", cols="10")
            ws.append_row(["Date","Symbol","Pattern","Direction","Price",
                           "PRZ Low","PRZ High","TV Link"])
        return ws
    except Exception as e:
        log.error(f"Google Sheets init failed: {e}")
        return None

def append_to_gsheet(ws, results: list):
    if not ws or not results: return
    today = date.today().isoformat()
    rows = [[today, r.symbol.replace('.NS',''), r.pattern, r.direction,
             round(r.price,2), round(r.prz_low,2), round(r.prz_high,2),
             f"{TV_BASE}{r.symbol.replace('.NS','')}"] for r in results]
    try:
        ws.append_rows(rows, value_input_option='USER_ENTERED')
        log.info(f"Appended {len(rows)} rows to Google Sheets.")
    except Exception as e:
        log.error(f"Google Sheets append failed: {e}")


# ─── Email Summary ─────────────────────────────────────────────────────────────
def send_email_summary(total_scanned: int, results: list):
    if not EMAIL_FROM or not EMAIL_TO:
        log.warning("Email not configured.")
        return
    today = date.today().strftime("%d %b %Y")
    subject = f"NSE Harmonic Scanner — {today} | {len(results)} Alerts"

    rows_html = ""
    for r in results:
        sym = r.symbol.replace('.NS','')
        color = '#26a69a' if r.direction == 'Bullish' else '#ef5350'
        rows_html += (
            f"<tr><td><a href='{TV_BASE}{sym}' style='color:#f0b90b'>{sym}</a></td>"
            f"<td>{r.pattern}</td><td style='color:{color}'>{r.direction}</td>"
            f"<td>₹{r.price:.2f}</td><td>₹{r.prz_low:.2f}–₹{r.prz_high:.2f}</td></tr>"
        )

    html = f"""<html><body style='background:#131722;color:#d1d4dc;font-family:Arial,sans-serif;padding:20px'>
    <h2 style='color:#f0b90b'>NSE Harmonic Scanner — {today}</h2>
    <p>Scanned: <b>{total_scanned}</b> &nbsp;|&nbsp; Detected: <b>{len(results)}</b></p>
    <table border='0' cellpadding='8' cellspacing='0'
           style='border-collapse:collapse;width:100%;background:#1e2130'>
    <tr style='background:#2a2e3f;color:#f0b90b'>
      <th>Symbol</th><th>Pattern</th><th>Direction</th><th>Price</th><th>PRZ</th>
    </tr>{rows_html}</table>
    <p style='color:#555;font-size:11px;margin-top:20px'>
      Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
    </body></html>"""

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = EMAIL_FROM
    msg['To']      = EMAIL_TO
    msg.attach(MIMEText(html, 'html'))
    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=30) as s:
            s.ehlo(); s.starttls()
            s.login(EMAIL_FROM, EMAIL_PASSWORD)
            s.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        log.info("Email sent.")
    except Exception as e:
        log.error(f"Email failed: {e}")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run_scanner():
    start = time.time()
    log.info("=" * 60)
    log.info(f"NSE Harmonic Scanner starting — {datetime.utcnow().isoformat()}")

    symbols = get_nse_symbols()
    log.info(f"Symbols to scan: {len(symbols)}")

    ohlc = fetch_all_ohlc(symbols)
    log.info(f"OHLC ready: {len(ohlc)} symbols")

    if len(ohlc) == 0:
        log.error("FATAL: 0 symbols downloaded. Yahoo Finance may be blocking this IP.")
        log.error("Try reducing MAX_WORKERS=2, BATCH_SIZE=10 in env vars.")
        send_email_summary(0, [])
        return

    all_results = []
    ws = get_gsheet()

    for sym, df in ohlc.items():
        hits = scan_symbol(sym, df)
        if not hits: continue
        log.info(f"  [{sym}] {len(hits)} pattern(s)")
        for result in hits:
            all_results.append(result)
            try:
                img = generate_chart(sym, df, result)
                send_telegram_photo(img, format_telegram_caption(result))
            except Exception as e:
                log.error(f"Chart/Telegram error {sym}: {e}")
            time.sleep(0.5)

    if all_results:
        append_to_gsheet(ws, all_results)

    send_email_summary(len(ohlc), all_results)

    elapsed = time.time() - start
    log.info(f"Done. {len(all_results)} patterns / {len(ohlc)} symbols. {elapsed:.1f}s")

    with open(f"results_{date.today().isoformat()}.json", 'w') as f:
        json.dump([asdict(r) for r in all_results], f, indent=2)


if __name__ == "__main__":
    run_scanner()
