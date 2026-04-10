"""
NSE Harmonic Pattern Scanner
Fully automated EOD scanner for ~2000 NSE stocks
"""

import os
import io
import time
import logging
import asyncio
import json
import smtplib
import warnings
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional
import traceback

import numpy as np
import pandas as pd
import yfinance as yf
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
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
GOOGLE_CREDS_JSON  = os.getenv('GOOGLE_CREDS_JSON', '')   # JSON string of service account
EMAIL_FROM         = os.getenv('EMAIL_FROM', '')
EMAIL_TO           = os.getenv('EMAIL_TO', '')
EMAIL_PASSWORD     = os.getenv('EMAIL_PASSWORD', '')
EMAIL_SMTP_HOST    = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
EMAIL_SMTP_PORT    = int(os.getenv('EMAIL_SMTP_PORT', '587'))

ZIGZAG_DEPTH       = int(os.getenv('ZIGZAG_DEPTH', '12'))
PRICE_PERIOD       = os.getenv('PRICE_PERIOD', '1y')
PRICE_INTERVAL     = os.getenv('PRICE_INTERVAL', '1d')
BATCH_SIZE         = int(os.getenv('BATCH_SIZE', '50'))
MAX_WORKERS        = int(os.getenv('MAX_WORKERS', '8'))
DOWNLOAD_RETRIES   = int(os.getenv('DOWNLOAD_RETRIES', '3'))
FIB_TOLERANCE      = float(os.getenv('FIB_TOLERANCE', '0.05'))   # ±5%

TV_BASE = "https://www.tradingview.com/chart/?symbol=NSE:"

# ─── Fibonacci Ratios ─────────────────────────────────────────────────────────
PATTERNS = {
    "Gartley": {
        "XB": (0.618, 0.618),
        "AC": (0.382, 0.886),
        "BD": (1.272, 1.618),
        "XD": (0.786, 0.786),
    },
    "Bat": {
        "XB": (0.382, 0.500),
        "AC": (0.382, 0.886),
        "BD": (1.618, 2.618),
        "XD": (0.886, 0.886),
    },
    "Butterfly": {
        "XB": (0.786, 0.786),
        "AC": (0.382, 0.886),
        "BD": (1.618, 2.618),
        "XD": (1.272, 1.618),
    },
    "Crab": {
        "XB": (0.382, 0.618),
        "AC": (0.382, 0.886),
        "BD": (2.240, 3.618),
        "XD": (1.618, 1.618),
    },
    "DeepCrab": {
        "XB": (0.886, 0.886),
        "AC": (0.382, 0.886),
        "BD": (2.000, 3.618),
        "XD": (1.618, 1.618),
    },
    "Shark": {
        "XB": (0.446, 0.618),
        "AC": (1.130, 1.618),
        "BD": (1.618, 2.240),
        "XD": (0.886, 1.130),
    },
    "Cypher": {
        "XB": (0.382, 0.618),
        "AC": (1.130, 1.414),
        "BD": (1.272, 2.000),  # retracement of XC
        "XD": (0.786, 0.786),
    },
    "ABCD": {
        "AB": (None, None),
        "BC": (0.382, 0.886),
        "CD": (1.272, 1.618),
        "AD": (None, None),
    },
}

# ─── Data Classes ─────────────────────────────────────────────────────────────
@dataclass
class HarmonicResult:
    symbol:    str
    pattern:   str
    direction: str        # "Bullish" | "Bearish"
    price:     float
    X: float; A: float; B: float; C: float; D: float
    x_idx: int; a_idx: int; b_idx: int; c_idx: int; d_idx: int
    prz_low:  float = 0.0
    prz_high: float = 0.0
    detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

# ─── NSE Symbol List ──────────────────────────────────────────────────────────
def get_nse_symbols() -> list[str]:
    """Return NSE .NS symbols. Fetches Nifty500 + MidSmall from NSE CSVs."""
    urls = [
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftymidsmallcap400list.csv",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    symbols = set()
    for url in urls:
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 200:
                    df = pd.read_csv(io.StringIO(r.text))
                    col = next((c for c in df.columns if 'symbol' in c.lower()), None)
                    if col:
                        symbols.update(df[col].dropna().str.strip().tolist())
                    break
            except Exception as e:
                log.warning(f"Symbol fetch attempt {attempt+1} failed: {e}")
                time.sleep(2)

    if not symbols:
        log.warning("Could not fetch symbols; using fallback list.")
        symbols = {
            "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","WIPRO","AXISBANK",
            "LT","BAJFINANCE","BHARTIARTL","ASIANPAINT","MARUTI","TITAN","ULTRACEMCO",
            "SUNPHARMA","TECHM","HCLTECH","POWERGRID","NTPC","ONGC","IOC","BPCL",
            "COALINDIA","GRASIM","INDUSINDBK","KOTAKBANK","NESTLEIND","ADANIENT",
        }
    return [f"{s}.NS" for s in sorted(symbols)]


# ─── Data Download ────────────────────────────────────────────────────────────
def download_batch(symbols: list[str], period: str, interval: str) -> dict[str, pd.DataFrame]:
    """Download OHLCV for a batch; returns {symbol: df}."""
    result = {}
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            raw = yf.download(
                symbols,
                period=period,
                interval=interval,
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
                timeout=30,
            )
            if raw.empty:
                break
            for sym in symbols:
                try:
                    if len(symbols) == 1:
                        df = raw.copy()
                    else:
                        df = raw[sym].copy()
                    df.dropna(subset=['Close'], inplace=True)
                    if len(df) >= 30:
                        result[sym] = df
                except Exception:
                    pass
            break
        except Exception as e:
            log.warning(f"Batch download attempt {attempt+1} failed: {e}")
            time.sleep(5 * (attempt + 1))
    return result


def fetch_all_ohlc(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Parallel batched download of all symbols."""
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    all_data = {}
    log.info(f"Downloading {len(symbols)} symbols in {len(batches)} batches...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(download_batch, b, PRICE_PERIOD, PRICE_INTERVAL): b for b in batches}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                all_data.update(fut.result())
            except Exception as e:
                log.error(f"Batch failed: {e}")
            if i % 10 == 0:
                log.info(f"  {i}/{len(batches)} batches done")

    log.info(f"Downloaded data for {len(all_data)} symbols.")
    return all_data


# ─── ZigZag ───────────────────────────────────────────────────────────────────
def zigzag_pivots(df: pd.DataFrame, depth: int = ZIGZAG_DEPTH) -> list[tuple[int, float]]:
    """
    Returns list of (index_position, price) pivot points.
    Uses a rolling window approach for speed.
    """
    highs = df['High'].values
    lows  = df['Low'].values
    n = len(highs)
    if n < depth * 2:
        return []

    pivots = []
    last_pivot_type = None   # 'H' or 'L'
    last_pivot_idx  = 0
    last_pivot_val  = 0.0

    # seed
    for i in range(depth, n - depth):
        is_high = all(highs[i] >= highs[i-j] for j in range(1, depth+1)) and \
                  all(highs[i] >= highs[i+j] for j in range(1, depth+1))
        is_low  = all(lows[i]  <= lows[i-j]  for j in range(1, depth+1)) and \
                  all(lows[i]  <= lows[i+j]  for j in range(1, depth+1))

        if is_high and last_pivot_type != 'H':
            if last_pivot_type == 'L' or not pivots:
                pivots.append((i, highs[i], 'H'))
                last_pivot_type = 'H'
                last_pivot_idx  = i
                last_pivot_val  = highs[i]
            elif highs[i] > last_pivot_val:
                pivots[-1] = (i, highs[i], 'H')
                last_pivot_val = highs[i]

        elif is_low and last_pivot_type != 'L':
            if last_pivot_type == 'H' or not pivots:
                pivots.append((i, lows[i], 'L'))
                last_pivot_type = 'L'
                last_pivot_idx  = i
                last_pivot_val  = lows[i]
            elif lows[i] < last_pivot_val:
                pivots[-1] = (i, lows[i], 'L')
                last_pivot_val = lows[i]

    return [(p[0], p[1]) for p in pivots]


# ─── Fibonacci Helpers ────────────────────────────────────────────────────────
def fib_ratio(a: float, b: float, c: float) -> float:
    """Retrace of B from A relative to A→C move."""
    if abs(c - a) < 1e-9:
        return 0.0
    return abs(b - a) / abs(c - a)

def in_range(val: float, lo: float, hi: float, tol: float = FIB_TOLERANCE) -> bool:
    if lo is None:
        return True
    lo_adj = lo * (1 - tol)
    hi_adj = hi * (1 + tol)
    return lo_adj <= val <= hi_adj

def compute_prz(X: float, A: float, B: float, C: float,
                pattern: str, bullish: bool) -> tuple[float, float]:
    """Approximate PRZ band."""
    try:
        r = PATTERNS[pattern]
        xd_lo, xd_hi = r.get("XD", (None, None))
        if xd_lo is None:
            return (C * 0.99, C * 1.01)
        move = abs(A - X)
        if bullish:
            d_lo = A - move * xd_hi
            d_hi = A - move * xd_lo
        else:
            d_lo = A + move * xd_lo
            d_hi = A + move * xd_hi
        return (min(d_lo, d_hi), max(d_lo, d_hi))
    except Exception:
        return (C * 0.98, C * 1.02)


# ─── Pattern Matching ─────────────────────────────────────────────────────────
def match_xabcd(pivots: list[tuple], pattern: str) -> list[HarmonicResult]:
    """Scan pivot windows for XABCD harmonic patterns."""
    results = []
    n = len(pivots)
    if n < 5:
        return results

    ratios = PATTERNS[pattern]

    for i in range(n - 4):
        for combo in [(i, i+1, i+2, i+3, i+4)]:
            xi, ai, bi, ci, di = combo
            X_idx, X = pivots[xi]
            A_idx, A = pivots[ai]
            B_idx, B = pivots[bi]
            C_idx, C = pivots[ci]
            D_idx, D = pivots[di]

            # Direction: bullish if X<A (upswing X→A)
            bullish = A > X

            # ── ABCD pattern ──────────────────────────────────────────
            if pattern == "ABCD":
                bc_ret = fib_ratio(A, B, C) if not bullish else fib_ratio(A, B, C)
                # Actually for ABCD we use A=X,B=A,C=B,D=C in 4-point
                # Redefine for 4 points
                A2, B2, C2, D2 = X, A, B, C
                bc_r = fib_ratio(A2, C2, B2)
                lo, hi = ratios["BC"]
                if not in_range(bc_r, lo, hi):
                    continue
                cd_ext = abs(D2 - C2) / (abs(B2 - A2) + 1e-9)
                lo2, hi2 = ratios["CD"]
                if not in_range(cd_ext, lo2, hi2):
                    continue
                direction = "Bullish" if A2 < B2 else "Bearish"
                prz = (D2 * 0.99, D2 * 1.01)
                results.append(HarmonicResult(
                    symbol="", pattern=pattern, direction=direction, price=D2,
                    X=A2, A=B2, B=C2, C=D2, D=D2,
                    x_idx=X_idx, a_idx=A_idx, b_idx=B_idx, c_idx=C_idx, d_idx=D_idx,
                    prz_low=prz[0], prz_high=prz[1],
                ))
                continue

            # ── XB retracement ────────────────────────────────────────
            xb_lo, xb_hi = ratios["XB"]
            xb_r = fib_ratio(X, B, A)
            if not in_range(xb_r, xb_lo, xb_hi):
                continue

            # ── AC retracement ────────────────────────────────────────
            ac_lo, ac_hi = ratios["AC"]
            ac_r = fib_ratio(A, C, B)
            if not in_range(ac_r, ac_lo, ac_hi):
                continue

            # ── BD extension ──────────────────────────────────────────
            bd_lo, bd_hi = ratios["BD"]
            bd_r = abs(D - B) / (abs(C - B) + 1e-9)
            if not in_range(bd_r, bd_lo, bd_hi):
                continue

            # ── XD retracement ────────────────────────────────────────
            xd_lo, xd_hi = ratios["XD"]
            xd_r = fib_ratio(X, D, A)
            if not in_range(xd_r, xd_lo, xd_hi):
                continue

            direction  = "Bullish" if bullish else "Bearish"
            prz_lo, prz_hi = compute_prz(X, A, B, C, pattern, bullish)

            # Price must be near D (within PRZ)
            if not (prz_lo * 0.97 <= D <= prz_hi * 1.03):
                continue

            results.append(HarmonicResult(
                symbol="", pattern=pattern, direction=direction, price=D,
                X=X, A=A, B=B, C=C, D=D,
                x_idx=X_idx, a_idx=A_idx, b_idx=B_idx, c_idx=C_idx, d_idx=D_idx,
                prz_low=prz_lo, prz_high=prz_hi,
            ))

    return results


def scan_symbol(symbol: str, df: pd.DataFrame) -> list[HarmonicResult]:
    """Run all pattern detectors on a single symbol."""
    detected = []
    try:
        pivots = zigzag_pivots(df, ZIGZAG_DEPTH)
        if len(pivots) < 5:
            return detected
        for pattern in PATTERNS:
            matches = match_xabcd(pivots, pattern)
            for m in matches:
                m.symbol = symbol
                # Filter: D must be within last 3 bars
                last_bar = len(df) - 1
                if last_bar - m.d_idx <= 3:
                    detected.append(m)
    except Exception as e:
        log.debug(f"scan_symbol {symbol} error: {e}")
    return detected


# ─── Charting ─────────────────────────────────────────────────────────────────
STYLE_TV = mpf.make_mpf_style(
    base_mpf_style='nightclouds',
    marketcolors=mpf.make_marketcolors(
        up='#26a69a', down='#ef5350',
        edge='inherit', wick='inherit', volume='in',
    ),
    facecolor='#131722',
    figcolor='#131722',
    gridcolor='#1e2130',
    gridstyle='-',
)

def generate_chart(symbol: str, df: pd.DataFrame, result: HarmonicResult) -> bytes:
    """Generate mplfinance chart with XABCD overlay. Returns PNG bytes."""
    tail = 80
    df_plot = df.iloc[-tail:].copy()

    # Map absolute indices to relative
    offset = len(df) - tail
    points = {
        'X': (result.x_idx - offset, result.X),
        'A': (result.a_idx - offset, result.A),
        'B': (result.b_idx - offset, result.B),
        'C': (result.c_idx - offset, result.C),
        'D': (result.d_idx - offset, result.D),
    }
    # Clamp to visible range
    points = {k: (max(0, min(v[0], tail-1)), v[1]) for k, v in points.items()}

    fig, axes = mpf.plot(
        df_plot,
        type='candle',
        style=STYLE_TV,
        title=f' {symbol}  |  {result.pattern}  {result.direction}',
        volume=True,
        returnfig=True,
        figsize=(12, 7),
        tight_layout=True,
    )
    ax = axes[0]

    # Draw pattern lines X→A→B→C→D
    order = ['X', 'A', 'B', 'C', 'D']
    xs = [points[p][0] for p in order]
    ys = [points[p][1] for p in order]
    ax.plot(xs, ys, 'o-', color='#f0b90b', linewidth=1.5, markersize=4, zorder=5)

    for label, (ix, price) in points.items():
        ax.annotate(
            label, xy=(ix, price), xytext=(0, 10 if label in ('A','C') else -15),
            textcoords='offset points', color='#f0b90b',
            fontsize=9, fontweight='bold', ha='center',
        )

    # PRZ shading
    ax.axhspan(result.prz_low, result.prz_high, alpha=0.15,
               color='#26a69a' if result.direction == 'Bullish' else '#ef5350')

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=130, bbox_inches='tight',
                facecolor='#131722', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram_photo(image_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured.")
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
    tv_url = f"{TV_BASE}{sym_clean}"
    arrow  = '🟢' if r.direction == 'Bullish' else '🔴'
    return (
        f"{arrow} <b>{r.pattern}</b> — {r.direction}\n"
        f"📌 <a href='{tv_url}'>{sym_clean}</a>\n"
        f"💰 Price: <b>₹{r.price:.2f}</b>\n"
        f"📊 PRZ: ₹{r.prz_low:.2f} – ₹{r.prz_high:.2f}"
    )


# ─── Google Sheets ────────────────────────────────────────────────────────────
def get_gsheet():
    if not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        return None
    try:
        import json as _json
        creds_dict = _json.loads(GOOGLE_CREDS_JSON)
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = sh.worksheet("Alerts")
        except Exception:
            ws = sh.add_worksheet(title="Alerts", rows="1000", cols="10")
            ws.append_row(["Date", "Symbol", "Pattern", "Direction", "Price",
                           "PRZ Low", "PRZ High", "TV Link"])
        return ws
    except Exception as e:
        log.error(f"Google Sheets init failed: {e}")
        return None

def append_to_gsheet(ws, results: list[HarmonicResult]):
    if not ws:
        return
    today = date.today().isoformat()
    rows = []
    for r in results:
        sym_clean = r.symbol.replace('.NS', '')
        rows.append([
            today,
            sym_clean,
            r.pattern,
            r.direction,
            round(r.price, 2),
            round(r.prz_low, 2),
            round(r.prz_high, 2),
            f"{TV_BASE}{sym_clean}",
        ])
    if rows:
        try:
            ws.append_rows(rows, value_input_option='USER_ENTERED')
            log.info(f"Appended {len(rows)} rows to Google Sheets.")
        except Exception as e:
            log.error(f"Google Sheets append failed: {e}")


# ─── Email Summary ─────────────────────────────────────────────────────────────
def send_email_summary(total_scanned: int, results: list[HarmonicResult]):
    if not EMAIL_FROM or not EMAIL_TO:
        log.warning("Email not configured.")
        return
    today = date.today().strftime("%d %b %Y")
    subject = f"NSE Harmonic Scanner — {today} | {len(results)} Alerts"

    rows_html = ""
    for r in results:
        sym_clean = r.symbol.replace('.NS', '')
        tv_url = f"{TV_BASE}{sym_clean}"
        color  = '#26a69a' if r.direction == 'Bullish' else '#ef5350'
        rows_html += (
            f"<tr>"
            f"<td><a href='{tv_url}' style='color:#f0b90b'>{sym_clean}</a></td>"
            f"<td>{r.pattern}</td>"
            f"<td style='color:{color}'>{r.direction}</td>"
            f"<td>₹{r.price:.2f}</td>"
            f"<td>₹{r.prz_low:.2f} – ₹{r.prz_high:.2f}</td>"
            f"</tr>"
        )

    html = f"""
    <html><body style='background:#131722;color:#d1d4dc;font-family:Arial,sans-serif;padding:20px'>
    <h2 style='color:#f0b90b'>NSE Harmonic Scanner — {today}</h2>
    <p>Symbols scanned: <b>{total_scanned}</b> &nbsp;|&nbsp; Patterns detected: <b>{len(results)}</b></p>
    <table border='0' cellpadding='8' cellspacing='0'
           style='border-collapse:collapse;width:100%;background:#1e2130;border-radius:8px'>
    <tr style='background:#2a2e3f;color:#f0b90b'>
      <th>Symbol</th><th>Pattern</th><th>Direction</th><th>Price</th><th>PRZ</th>
    </tr>
    {rows_html}
    </table>
    <p style='color:#555;font-size:11px;margin-top:20px'>
      Generated by NSE Harmonic Scanner • {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
    </body></html>
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = EMAIL_FROM
    msg['To']      = EMAIL_TO
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        log.info("Email summary sent.")
    except Exception as e:
        log.error(f"Email send failed: {e}")


# ─── Main Scanner ──────────────────────────────────────────────────────────────
def run_scanner():
    start_ts = time.time()
    log.info("=" * 60)
    log.info(f"NSE Harmonic Scanner starting at {datetime.utcnow().isoformat()}")

    symbols = get_nse_symbols()
    log.info(f"Total symbols to scan: {len(symbols)}")

    ohlc_data = fetch_all_ohlc(symbols)
    log.info(f"OHLC data ready for {len(ohlc_data)} symbols")

    all_results: list[HarmonicResult] = []

    ws = get_gsheet()

    for sym, df in ohlc_data.items():
        hits = scan_symbol(sym, df)
        if not hits:
            continue
        log.info(f"  [{sym}] {len(hits)} pattern(s) found")

        for result in hits:
            all_results.append(result)
            try:
                img = generate_chart(sym, df, result)
                caption = format_telegram_caption(result)
                send_telegram_photo(img, caption)
            except Exception as e:
                log.error(f"Chart/Telegram error for {sym}: {e}")
            time.sleep(0.5)   # rate limit

    if all_results and ws:
        append_to_gsheet(ws, all_results)

    send_email_summary(len(ohlc_data), all_results)

    elapsed = time.time() - start_ts
    log.info(f"Scan complete. {len(all_results)} patterns across {len(ohlc_data)} symbols. "
             f"Time: {elapsed:.1f}s")

    # Save JSON log
    log_path = f"results_{date.today().isoformat()}.json"
    with open(log_path, 'w') as f:
        json.dump([asdict(r) for r in all_results], f, indent=2)
    log.info(f"Results saved to {log_path}")


if __name__ == "__main__":
    run_scanner()
