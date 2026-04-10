"""
NSE Harmonic Pattern Scanner v3
Data: NSE India API (primary) + Stooq (fallback) — NO Yahoo Finance
Both sources work reliably from GitHub Actions
"""

import os, io, time, logging, json, smtplib, warnings, random
from datetime import datetime, date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional

import numpy as np
import pandas as pd
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
    handlers=[logging.FileHandler('scanner.log'), logging.StreamHandler()]
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

ZIGZAG_DEPTH   = int(os.getenv('ZIGZAG_DEPTH', '12'))
MAX_WORKERS    = int(os.getenv('MAX_WORKERS', '6'))
FIB_TOLERANCE  = float(os.getenv('FIB_TOLERANCE', '0.05'))
LOOKBACK_DAYS  = int(os.getenv('LOOKBACK_DAYS', '365'))   # 1 year of daily data

TV_BASE = "https://www.tradingview.com/chart/?symbol=NSE:"

# ─── HTTP Session ─────────────────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
}

def make_session(headers: dict = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=2,
                  status_forcelist=[500, 502, 503, 504],
                  allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(headers or NSE_HEADERS)
    return session


# ─── NSE Symbol Fetcher ───────────────────────────────────────────────────────
def get_nse_symbols() -> list[str]:
    """Fetch symbols from NSE India index CSVs."""
    urls = [
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftymidsmallcap400list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    ]
    session = make_session()
    symbols = set()
    for url in urls:
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                col = next((c for c in df.columns if 'symbol' in c.lower()), None)
                if col:
                    symbols.update(df[col].dropna().str.strip().tolist())
                    log.info(f"  Loaded {len(symbols)} symbols so far from {url.split('/')[-1]}")
        except Exception as e:
            log.warning(f"Symbol URL failed: {e}")
        time.sleep(0.5)

    if not symbols:
        log.warning("NSE symbol fetch failed — using curated fallback list")
        symbols = {
            "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","WIPRO","AXISBANK",
            "LT","BAJFINANCE","BHARTIARTL","ASIANPAINT","MARUTI","TITAN","ULTRACEMCO",
            "SUNPHARMA","TECHM","HCLTECH","POWERGRID","NTPC","ONGC","IOC","BPCL",
            "COALINDIA","GRASIM","INDUSINDBK","KOTAKBANK","NESTLEIND","ADANIENT",
            "BAJAJFINSV","HINDALCO","TATASTEEL","JSWSTEEL","DRREDDY","CIPLA","DIVISLAB",
            "EICHERMOT","TATACONSUM","APOLLOHOSP","BRITANNIA","DABUR","MARICO",
            "PIDILITIND","HAVELLS","SIEMENS","ABB","BOSCHLTD","CUMMINSIND","THERMAX",
            "ADANIPORTS","ADANIPOWER","ADANIGREEN","ATGL","HINDUNILVR","COLPAL",
            "GODREJCP","MAHABANK","BANKBARODA","CANBK","PNB","UNIONBANK","BANKINDIA",
            "IRFC","RECLTD","PFC","IRCTC","RVNL","HAL","BEL","BDL","COCHINSHIP",
            "MAZAGON","GRSE","BEML","TITAGARH","RAILTEL","NBCC","NHPC","SJVN",
            "TATAPOWER","NTPCGREEN","SUZLON","INOXWIND","POLYCAB","KEI","FINCABLES",
            "APARINDS","HDFCLIFE","SBILIFE","ICICIGI","ICICIPRULI","LICI","STARHEALTH",
            "MUTHOOTFIN","BAJAJHFL","LICHSGFIN","CHOLAFIN","BAJAJHLDNG",
        }
    return sorted(symbols)   # Return plain symbols (no .NS suffix)


# ─── DATA SOURCE 1: NSE India Historical API ──────────────────────────────────
NSE_HIST_URL = "https://www.nseindia.com/api/historical/cm/equity"

def fetch_nse_direct(symbol: str, session: requests.Session,
                     days: int = LOOKBACK_DAYS) -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV from NSE India's own historical API.
    No rate limits for reasonable usage.
    """
    end   = date.today()
    start = end - timedelta(days=days + 30)   # buffer for weekends/holidays

    params = {
        "symbol": symbol,
        "series": "EQ",
        "from": start.strftime("%d-%m-%Y"),
        "to":   end.strftime("%d-%m-%Y"),
    }
    try:
        # First hit the main page to get cookies
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(0.3)

        r = session.get(NSE_HIST_URL, params=params, timeout=20)
        if r.status_code != 200:
            return None

        data = r.json()
        records = data.get("data", [])
        if not records:
            return None

        rows = []
        for rec in records:
            try:
                rows.append({
                    "Date":   pd.to_datetime(rec["CH_TIMESTAMP"]),
                    "Open":   float(rec["CH_OPENING_PRICE"]),
                    "High":   float(rec["CH_TRADE_HIGH_PRICE"]),
                    "Low":    float(rec["CH_TRADE_LOW_PRICE"]),
                    "Close":  float(rec["CH_CLOSING_PRICE"]),
                    "Volume": float(rec["CH_TOT_TRADED_QTY"]),
                })
            except (KeyError, ValueError, TypeError):
                continue

        if not rows:
            return None

        df = pd.DataFrame(rows).set_index("Date").sort_index()
        df = df[df["Close"] > 0].dropna()
        return df if len(df) >= 30 else None

    except Exception as e:
        log.debug(f"NSE direct {symbol}: {e}")
        return None


# ─── DATA SOURCE 2: Stooq (fallback) ─────────────────────────────────────────
STOOQ_URL = "https://stooq.com/q/d/l/"

def fetch_stooq(symbol: str, session: requests.Session,
                days: int = LOOKBACK_DAYS) -> Optional[pd.DataFrame]:
    """
    Stooq.com — free EOD data, no auth needed, works from any IP.
    Symbol format: RELIANCE.NS → RELIANCE.IN
    """
    stooq_sym = f"{symbol}.IN"   # NSE stocks on Stooq use .IN suffix
    end   = date.today()
    start = end - timedelta(days=days + 30)

    params = {
        "s": stooq_sym,
        "d1": start.strftime("%Y%m%d"),
        "d2": end.strftime("%Y%m%d"),
        "i": "d",   # daily
    }
    try:
        r = session.get(STOOQ_URL, params=params, timeout=20,
                        headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or len(r.text) < 50:
            return None

        df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])
        if df.empty or "Close" not in df.columns:
            return None

        df = df.set_index("Date").sort_index()
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        df = df[df["Close"] > 0]
        return df if len(df) >= 30 else None

    except Exception as e:
        log.debug(f"Stooq {symbol}: {e}")
        return None


# ─── DATA SOURCE 3: NSE CSV Download (second fallback) ────────────────────────
def fetch_nse_csv(symbol: str, session: requests.Session,
                  days: int = LOOKBACK_DAYS) -> Optional[pd.DataFrame]:
    """
    NSE Bhavcopy approach: direct CSV download from NSE.
    Works as a final fallback.
    """
    end   = date.today()
    start = end - timedelta(days=days + 30)

    url = (f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}"
           f"&series=[%22EQ%22]&from={start.strftime('%d-%m-%Y')}"
           f"&to={end.strftime('%d-%m-%Y')}&csv=true")
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.2)
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            return None

        df = pd.read_csv(io.StringIO(r.text))
        if df.empty: return None

        # NSE CSV column mapping
        col_map = {
            "Date": "Date", "open": "Open", "high": "High",
            "low": "Low", "close": "Close", "volume": "Volume",
            "CH_TIMESTAMP": "Date", "CH_OPENING_PRICE": "Open",
            "CH_TRADE_HIGH_PRICE": "High", "CH_TRADE_LOW_PRICE": "Low",
            "CH_CLOSING_PRICE": "Close", "CH_TOT_TRADED_QTY": "Volume",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
        if not all(c in df.columns for c in needed):
            return None

        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)
        df = df.set_index("Date").sort_index()
        df = df[["Open", "High", "Low", "Close", "Volume"]].apply(
            pd.to_numeric, errors='coerce').dropna()
        return df if len(df) >= 30 else None

    except Exception as e:
        log.debug(f"NSE CSV {symbol}: {e}")
        return None


# ─── Unified Downloader ───────────────────────────────────────────────────────
def fetch_symbol(symbol: str) -> tuple[str, Optional[pd.DataFrame]]:
    """
    Try data sources in order:
    1. NSE India API  (best quality, official)
    2. Stooq          (reliable fallback, no rate limits)
    3. NSE CSV        (last resort)
    """
    nse_session   = make_session(NSE_HEADERS)
    plain_session = make_session()

    # Source 1: NSE Direct
    df = fetch_nse_direct(symbol, nse_session)
    if df is not None and len(df) >= 30:
        log.debug(f"{symbol}: NSE API OK ({len(df)} bars)")
        return symbol, df

    time.sleep(random.uniform(0.2, 0.5))

    # Source 2: Stooq
    df = fetch_stooq(symbol, plain_session)
    if df is not None and len(df) >= 30:
        log.debug(f"{symbol}: Stooq OK ({len(df)} bars)")
        return symbol, df

    time.sleep(random.uniform(0.2, 0.5))

    # Source 3: NSE CSV
    df = fetch_nse_csv(symbol, nse_session)
    if df is not None and len(df) >= 30:
        log.debug(f"{symbol}: NSE CSV OK ({len(df)} bars)")
        return symbol, df

    log.warning(f"{symbol}: all sources failed")
    return symbol, None


def fetch_all_ohlc(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Parallel download with progress logging."""
    log.info(f"Fetching {len(symbols)} symbols (workers={MAX_WORKERS})...")
    all_data = {}
    failed   = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_symbol, sym): sym for sym in symbols}
        done = 0
        for fut in as_completed(futures):
            sym, df = fut.result()
            done += 1
            if df is not None:
                all_data[sym] = df
            else:
                failed.append(sym)
            if done % 50 == 0 or done == len(symbols):
                log.info(f"  Progress: {done}/{len(symbols)} "
                         f"({len(all_data)} OK, {len(failed)} failed)")

    log.info(f"Download complete: {len(all_data)}/{len(symbols)} symbols")
    if failed:
        log.info(f"Failed ({len(failed)}): {failed[:20]}{'...' if len(failed)>20 else ''}")
    return all_data


# ─── Fibonacci Ratios ─────────────────────────────────────────────────────────
PATTERNS = {
    "Gartley":   {"XB":(0.618,0.618),"AC":(0.382,0.886),"BD":(1.272,1.618),"XD":(0.786,0.786)},
    "Bat":       {"XB":(0.382,0.500),"AC":(0.382,0.886),"BD":(1.618,2.618),"XD":(0.886,0.886)},
    "Butterfly": {"XB":(0.786,0.786),"AC":(0.382,0.886),"BD":(1.618,2.618),"XD":(1.272,1.618)},
    "Crab":      {"XB":(0.382,0.618),"AC":(0.382,0.886),"BD":(2.240,3.618),"XD":(1.618,1.618)},
    "DeepCrab":  {"XB":(0.886,0.886),"AC":(0.382,0.886),"BD":(2.000,3.618),"XD":(1.618,1.618)},
    "Shark":     {"XB":(0.446,0.618),"AC":(1.130,1.618),"BD":(1.618,2.240),"XD":(0.886,1.130)},
    "Cypher":    {"XB":(0.382,0.618),"AC":(1.130,1.414),"BD":(1.272,2.000),"XD":(0.786,0.786)},
    "ABCD":      {"AB":(None,None),  "BC":(0.382,0.886),"CD":(1.272,1.618),"AD":(None,None)},
}

@dataclass
class HarmonicResult:
    symbol: str; pattern: str; direction: str; price: float
    X: float; A: float; B: float; C: float; D: float
    x_idx: int; a_idx: int; b_idx: int; c_idx: int; d_idx: int
    prz_low: float = 0.0; prz_high: float = 0.0
    detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

def fib_ratio(a, b, c):
    return 0.0 if abs(c-a) < 1e-9 else abs(b-a)/abs(c-a)

def in_range(val, lo, hi, tol=FIB_TOLERANCE):
    return True if lo is None else lo*(1-tol) <= val <= hi*(1+tol)

def compute_prz(X, A, B, C, pattern, bullish):
    try:
        xd_lo, xd_hi = PATTERNS[pattern].get("XD", (None, None))
        if xd_lo is None: return (C*0.99, C*1.01)
        move = abs(A-X)
        if bullish: return sorted([A-move*xd_hi, A-move*xd_lo])
        return sorted([A+move*xd_lo, A+move*xd_hi])
    except: return (C*0.98, C*1.02)


# ─── ZigZag ───────────────────────────────────────────────────────────────────
def zigzag_pivots(df: pd.DataFrame, depth: int = ZIGZAG_DEPTH) -> list:
    highs, lows = df['High'].values, df['Low'].values
    n = len(highs)
    if n < depth*2: return []
    pivots, last_type = [], None
    for i in range(depth, n-depth):
        is_h = all(highs[i]>=highs[i-j] for j in range(1,depth+1)) and \
               all(highs[i]>=highs[i+j] for j in range(1,depth+1))
        is_l = all(lows[i] <=lows[i-j]  for j in range(1,depth+1)) and \
               all(lows[i] <=lows[i+j]  for j in range(1,depth+1))
        if is_h and last_type != 'H':
            if pivots and last_type=='H' and highs[i]>pivots[-1][1]:
                pivots[-1] = (i, highs[i])
            else: pivots.append((i, highs[i]))
            last_type = 'H'
        elif is_l and last_type != 'L':
            if pivots and last_type=='L' and lows[i]<pivots[-1][1]:
                pivots[-1] = (i, lows[i])
            else: pivots.append((i, lows[i]))
            last_type = 'L'
    return pivots


# ─── Pattern Matching ─────────────────────────────────────────────────────────
def match_xabcd(pivots: list, pattern: str) -> list:
    results, n = [], len(pivots)
    if n < 5: return results
    r = PATTERNS[pattern]
    for i in range(n-4):
        (Xi,X),(Ai,A),(Bi,B),(Ci,C),(Di,D) = pivots[i:i+5]
        bullish = A > X

        if pattern == "ABCD":
            if not in_range(fib_ratio(X,B,A), *r["BC"]): continue
            if not in_range(abs(C-B)/(abs(A-X)+1e-9), *r["CD"]): continue
            results.append(HarmonicResult(
                symbol="",pattern=pattern,direction="Bullish" if X<A else "Bearish",
                price=C,X=X,A=A,B=B,C=C,D=C,
                x_idx=Xi,a_idx=Ai,b_idx=Bi,c_idx=Ci,d_idx=Ci,
                prz_low=C*0.99,prz_high=C*1.01))
            continue

        if not in_range(fib_ratio(X,B,A), *r["XB"]): continue
        if not in_range(fib_ratio(A,C,B), *r["AC"]): continue
        if not in_range(abs(D-B)/(abs(C-B)+1e-9), *r["BD"]): continue
        if not in_range(fib_ratio(X,D,A), *r["XD"]): continue

        prz_lo, prz_hi = compute_prz(X,A,B,C,pattern,bullish)
        if not (prz_lo*0.97 <= D <= prz_hi*1.03): continue

        results.append(HarmonicResult(
            symbol="",pattern=pattern,
            direction="Bullish" if bullish else "Bearish",
            price=D,X=X,A=A,B=B,C=C,D=D,
            x_idx=Xi,a_idx=Ai,b_idx=Bi,c_idx=Ci,d_idx=Di,
            prz_low=prz_lo,prz_high=prz_hi))
    return results


def scan_symbol(symbol: str, df: pd.DataFrame) -> list:
    try:
        pivots = zigzag_pivots(df, ZIGZAG_DEPTH)
        if len(pivots) < 5: return []
        last = len(df)-1
        return [m for pat in PATTERNS
                for m in match_xabcd(pivots, pat)
                if (setattr(m, 'symbol', symbol) or True) and last-m.d_idx<=3]
    except Exception as e:
        log.debug(f"scan {symbol}: {e}")
        return []


# ─── Chart ────────────────────────────────────────────────────────────────────
STYLE_TV = mpf.make_mpf_style(
    base_mpf_style='nightclouds',
    marketcolors=mpf.make_marketcolors(up='#26a69a',down='#ef5350',
        edge='inherit',wick='inherit',volume='in'),
    facecolor='#131722',figcolor='#131722',gridcolor='#1e2130',gridstyle='-')

def generate_chart(symbol: str, df: pd.DataFrame, r: HarmonicResult) -> bytes:
    tail = 80
    df_plot = df.iloc[-tail:].copy()
    off = len(df)-tail
    pts = {k:(max(0,min(v[0]-off,tail-1)),v[1]) for k,v in {
        'X':(r.x_idx,r.X),'A':(r.a_idx,r.A),'B':(r.b_idx,r.B),
        'C':(r.c_idx,r.C),'D':(r.d_idx,r.D)}.items()}

    fig, axes = mpf.plot(df_plot,type='candle',style=STYLE_TV,
        title=f' {symbol}  |  {r.pattern}  {r.direction}',
        volume=True,returnfig=True,figsize=(12,7),tight_layout=True)
    ax = axes[0]
    xs=[pts[p][0] for p in 'XABCD']; ys=[pts[p][1] for p in 'XABCD']
    ax.plot(xs,ys,'o-',color='#f0b90b',linewidth=1.5,markersize=4,zorder=5)
    for lbl,(ix,pr) in pts.items():
        ax.annotate(lbl,xy=(ix,pr),xytext=(0,10 if lbl in 'AC' else -15),
            textcoords='offset points',color='#f0b90b',fontsize=9,
            fontweight='bold',ha='center')
    ax.axhspan(r.prz_low,r.prz_high,alpha=0.15,
        color='#26a69a' if r.direction=='Bullish' else '#ef5350')
    buf = io.BytesIO()
    fig.savefig(buf,format='png',dpi=130,bbox_inches='tight',
        facecolor='#131722',edgecolor='none')
    plt.close(fig); buf.seek(0)
    return buf.read()


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram_photo(img: bytes, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    for _ in range(3):
        try:
            r = requests.post(url,data={'chat_id':TELEGRAM_CHAT_ID,
                'caption':caption,'parse_mode':'HTML'},
                files={'photo':('chart.png',img,'image/png')},timeout=30)
            if r.status_code==200: return True
        except: pass
        time.sleep(3)
    return False

def telegram_caption(r: HarmonicResult) -> str:
    s = r.symbol
    arrow = '🟢' if r.direction=='Bullish' else '🔴'
    return (f"{arrow} <b>{r.pattern}</b> — {r.direction}\n"
            f"📌 <a href='{TV_BASE}{s}'>{s}</a>\n"
            f"💰 Price: <b>₹{r.price:.2f}</b>\n"
            f"📊 PRZ: ₹{r.prz_low:.2f} – ₹{r.prz_high:.2f}")


# ─── Google Sheets ────────────────────────────────────────────────────────────
def get_gsheet():
    if not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID: return None
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(GOOGLE_CREDS_JSON),
            ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive'])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try: ws = sh.worksheet("Alerts")
        except:
            ws = sh.add_worksheet("Alerts",1000,10)
            ws.append_row(["Date","Symbol","Pattern","Direction","Price",
                           "PRZ Low","PRZ High","TV Link"])
        return ws
    except Exception as e:
        log.error(f"Sheets init failed: {e}"); return None

def append_gsheet(ws, results: list):
    if not ws or not results: return
    today = date.today().isoformat()
    rows = [[today,r.symbol,r.pattern,r.direction,
             round(r.price,2),round(r.prz_low,2),round(r.prz_high,2),
             f"{TV_BASE}{r.symbol}"] for r in results]
    try: ws.append_rows(rows,value_input_option='USER_ENTERED')
    except Exception as e: log.error(f"Sheets append failed: {e}")


# ─── Email ────────────────────────────────────────────────────────────────────
def send_email(total: int, results: list):
    if not EMAIL_FROM or not EMAIL_TO: return
    today = date.today().strftime("%d %b %Y")
    rows = "".join(
        f"<tr><td><a href='{TV_BASE}{r.symbol}' style='color:#f0b90b'>{r.symbol}</a></td>"
        f"<td>{r.pattern}</td>"
        f"<td style='color:{'#26a69a' if r.direction==\"Bullish\" else \"#ef5350\"}'>{r.direction}</td>"
        f"<td>₹{r.price:.2f}</td><td>₹{r.prz_low:.2f}–₹{r.prz_high:.2f}</td></tr>"
        for r in results)
    html = (f"<html><body style='background:#131722;color:#d1d4dc;font-family:Arial;padding:20px'>"
            f"<h2 style='color:#f0b90b'>NSE Harmonic Scanner — {today}</h2>"
            f"<p>Scanned:<b>{total}</b> | Detected:<b>{len(results)}</b></p>"
            f"<table cellpadding='8' style='border-collapse:collapse;width:100%;background:#1e2130'>"
            f"<tr style='background:#2a2e3f;color:#f0b90b'>"
            f"<th>Symbol</th><th>Pattern</th><th>Direction</th><th>Price</th><th>PRZ</th></tr>"
            f"{rows}</table>"
            f"<p style='color:#555;font-size:11px'>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>"
            f"</body></html>")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"NSE Harmonic Scanner — {today} | {len(results)} Alerts"
    msg['From'] = EMAIL_FROM; msg['To'] = EMAIL_TO
    msg.attach(MIMEText(html,'html'))
    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST,EMAIL_SMTP_PORT,timeout=30) as s:
            s.ehlo(); s.starttls(); s.login(EMAIL_FROM,EMAIL_PASSWORD)
            s.sendmail(EMAIL_FROM,EMAIL_TO,msg.as_string())
        log.info("Email sent.")
    except Exception as e: log.error(f"Email failed: {e}")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run_scanner():
    t0 = time.time()
    log.info("="*60)
    log.info(f"NSE Harmonic Scanner v3 — {datetime.utcnow().isoformat()}")
    log.info("Data sources: NSE India API → Stooq → NSE CSV (no Yahoo Finance)")

    symbols = get_nse_symbols()
    log.info(f"Symbols: {len(symbols)}")

    ohlc = fetch_all_ohlc(symbols)

    if not ohlc:
        log.error("0 symbols downloaded. Check network / data source availability.")
        send_email(0, [])
        return

    all_results, ws = [], get_gsheet()

    for sym, df in ohlc.items():
        hits = scan_symbol(sym, df)
        if not hits: continue
        log.info(f"  [{sym}] {len(hits)} pattern(s) detected")
        for r in hits:
            all_results.append(r)
            try:
                send_telegram_photo(generate_chart(sym, df, r), telegram_caption(r))
            except Exception as e:
                log.error(f"Chart/Telegram {sym}: {e}")
            time.sleep(0.3)

    if all_results: append_gsheet(ws, all_results)
    send_email(len(ohlc), all_results)

    elapsed = time.time()-t0
    log.info(f"Done — {len(all_results)} patterns / {len(ohlc)} symbols / {elapsed:.1f}s")
    with open(f"results_{date.today()}.json",'w') as f:
        json.dump([asdict(r) for r in all_results],f,indent=2)


if __name__ == "__main__":
    run_scanner()
