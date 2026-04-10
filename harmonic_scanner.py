"""
NSE Harmonic Pattern Scanner v4
Data: jugaad-data (NSE official, works on GitHub Actions) + yfinance as fallback
jugaad-data handles NSE cookie/session management correctly.
"""

import os, io, time, logging, json, smtplib, warnings, random
from datetime import datetime, date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional
import traceback

import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings('ignore')
logging.getLogger('jugaad_data').setLevel(logging.ERROR)

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

ZIGZAG_DEPTH  = int(os.getenv('ZIGZAG_DEPTH', '12'))
MAX_WORKERS   = int(os.getenv('MAX_WORKERS', '4'))
FIB_TOLERANCE = float(os.getenv('FIB_TOLERANCE', '0.05'))
LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '365'))

TV_BASE = "https://www.tradingview.com/chart/?symbol=NSE:"

# ─── NSE Symbol List ──────────────────────────────────────────────────────────
# Curated Nifty500 symbols - avoids dependency on NSE CSV download
NIFTY500_SYMBOLS = [
    "360ONE","3MINDIA","AADHARHFC","AARTIIND","AAVAS","ABB","ABBOTINDIA","ABCAPITAL",
    "ABDL","ABFRL","ABREL","ABLBL","ABSLAMC","ACC","ACE","ACMESOLAR","ACUTAAS",
    "ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ADANIPOWER","AEGISLOG",
    "AEGISVOPAK","AFFLE","AFCONS","AIAENG","AIIL","AJANTPHARM","AKZOINDIA","ALKEM",
    "AMBER","AMBUJACEM","ANANDRATHI","ANANTRAJ","ANGELONE","ANURAS","ANTHEM","APARINDS",
    "APLAPOLLO","APOLLOHOSP","APOLLOTYRE","APTUS","ARE&M","ASAHIINDIA","ASHOKLEY",
    "ASIANPAINT","ASTERDM","ASTRAL","ATHERENERG","ATGL","ATUL","AUBANK","AUROPHARMA",
    "AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJAJHFL","BAJAJHLDNG","BAJFINANCE",
    "BALKRISIND","BALRAMCHIN","BANDHANBNK","BANKINDIA","BANKBARODA","BATAINDIA",
    "BAYERCROP","BBTC","BDL","BEL","BEML","BERGEPAINT","BHARATFORG","BHARTIARTL",
    "BHARTIHEXA","BHEL","BIKAJI","BIOCON","BLS","BLUEJET","BLUESTARCO","BLUEDART",
    "BOSCHLTD","BPCL","BRIGADE","BSE","BSOFT","CAMS","CANFINHOME","CANBK","CANHLIFE",
    "CAPLIPOINT","CARBORUNIV","CARTRADE","CASTROLIND","CDSL","CEATLTD","CENTRALBK",
    "CESC","CGCL","CGPOWER","CHALET","CHAMBLFERT","CHENNPETRO","CHOLAFIN","CHOLAHLDNG",
    "CIPLA","CLEAN","COALINDIA","COCHINSHIP","COFORGE","COHANCE","COLPAL","CONCOR",
    "CONCORDBIO","COROMANDEL","CRAFTSMAN","CREDITACC","CRISIL","CROMPTON","CUB",
    "CUMMINSIND","CYIENT","DABUR","DALBHARAT","DATAPATTNS","DCMSHRIRAM","DEEPAKNTR",
    "DEEPAKFERT","DELHIVERY","DEVYANI","DIVISLAB","DIXON","DLF","DMART","DOMS",
    "DRREDDY","EICHERMOT","EIHOTEL","ELECON","ELGIEQUIP","EMAMILTD","EMCURE","EMMVEE",
    "ENDURANCE","ENGINERSIN","ENRIN","ESCORTS","ETERNAL","EXIDEIND","FACT","FINCABLES",
    "FIRSTCRY","FIVESTAR","FLUOROCHEM","FORCEMOT","FORTIS","FSL","GABRIEL","GAIL",
    "GALLANTT","GICRE","GILLETTE","GLAND","GLAXO","GLENMARK","GMRAIRPORT","GMDCLTD",
    "GODREJCP","GODREJIND","GODREJPROP","GODFRYPHLP","GODIGIT","GPIL","GRAPHITE",
    "GRASIM","GRAVITA","GRSE","GROWW","GSPL","GVT&D","HAL","HAVELLS","HCLTECH",
    "HDBFS","HDFCAMC","HDFCBANK","HDFCLIFE","HEG","HEROMOTOCO","HEXT","HFCL",
    "HINDCOPPER","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HOMEFIRST","HONASA",
    "HONAUT","HUDCO","HYUNDAI","ICICIBANK","ICICIAMC","ICICIGI","ICICIPRULI","IDBI",
    "IDFCFIRSTB","IEX","IFCI","IGIL","IGL","IKS","INDGN","INDHOTEL","INDIACEM",
    "INDIAMART","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INTELLECT","IPCALAB",
    "IRCTC","IREDA","IRFC","IRB","IRCON","ITC","ITCHOTELS","ITI","IIFL","J&KBANK",
    "JAINREC","JBCHEPHARM","JBMA","JIOFIN","JKCEMENT","JKTYRE","JMFINANCIL",
    "JINDALSAW","JINDALSTEL","JPPOWER","JSL","JSWCEMENT","JSWENERGY","JSWINFRA",
    "JSWSTEEL","JUBLFOOD","JUBLINGREA","JUBLPHARMA","JWL","JYOTICNC","KAJARIACER",
    "KALYANKJIL","KARURVYSYA","KAYNES","KEC","KEI","KFINTECH","KIMS","KIRLOSENG",
    "KOTAKBANK","KPIL","KPITTECH","KPRMILL","LALPATHLAB","LATENTVIEW","LAURUSLABS",
    "LT","LTFOODS","LTF","LTM","LTTS","LEMONTREE","LENSKART","LGEINDIA","LICHSGFIN",
    "LICI","LINDEINDIA","LLOYDSME","LODHA","LUPIN","M&M","M&MFIN","MAHABANK",
    "MANKIND","MANAPPURAM","MAPMYINDIA","MARICO","MARUTI","MAXHEALTH","MAZDOCK",
    "MCX","MEDANTA","MEESHO","MFSL","MGL","MINDACORP","MMTC","MOTILALOFS","MOTHERSON",
    "MPHASIS","MRF","MRPL","MSUMI","MUTHOOTFIN","NAM-INDIA","NATCOPHARM","NATIONALUM",
    "NAUKRI","NAVA","NAVINFLUOR","NBCC","NCC","NESTLEIND","NETWEB","NEULANDLAB",
    "NEWGEN","NH","NHPC","NIACL","NIVABUPA","NLCINDIA","NMDC","NSLNISP","NTPC",
    "NTPCGREEN","NUVAMA","NUVOCO","NYKAA","OBEROIRLTY","OFSS","OIL","OLAELEC",
    "OLECTRA","ONGC","ONESOURCE","PAGEIND","PARADEEP","PATANJALI","PAYTM","PCBL",
    "PERSISTENT","PETRONET","PFC","PFIZER","PGEL","PHOENIXLTD","PIDILITIND","PIIND",
    "PINELABS","PIRAMALFIN","POLICYBZR","POLYCAB","POLYMED","PNB","PNBHOUSING",
    "POONAWALLA","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE","PTCIL","PVRINOX",
    "PWL","RADICO","RAILTEL","RAINBOW","RAMCOCEM","RECLTD","REDINGTON","RELIANCE",
    "RHIM","RITES","RBLBANK","RKFORGE","RRKABEL","RPOWER","RVNL","SAGILITY","SAIL",
    "SAILIFE","SAMMAANCAP","SAPPHIRE","SARDAEN","SAREGAMA","SBFC","SBICARD","SBILIFE",
    "SBIN","SCI","SCHAEFFLER","SCHNEIDER","SHREECEM","SHRIRAMFIN","SHYAMMETL",
    "SIEMENS","SIGNATURE","SJVN","SOBHA","SOLARINDS","SONACOMS","SONATSOFTW","SRF",
    "STARHEALTH","SUPREMEIND","SUNPHARMA","SUNTV","SUNDARMFIN","SUMICHEM","SUZLON",
    "SWANCORP","SWIGGY","SYNGENE","SYRMA","TATACAP","TATACHEM","TATACOMM","TATACONSUM",
    "TATAELXSI","TATAINVEST","TATAPOWER","TATASTEEL","TATATECH","TBOTEK","TCS",
    "TECHM","TEGA","TEJASNET","TENNIND","THERMAX","THELEELA","TIINDIA","TIMKEN",
    "TITAGARH","TITAN","TMCV","TMPV","TORNTPHARM","TORNTPOWER","TRAVELFOOD","TRENT",
    "TRIDENT","TRITURBINE","TTML","TVSMOTOR","UBL","UCOBANK","ULTRACEMCO","UNIONBANK",
    "UNITDSPR","UNOMINDA","UPL","URBANCO","USHAMART","UTIAMC","VBL","VEDL","VIJAYA",
    "VMM","VOLTAS","VTL","WAAREEENER","WELCORP","WELSPUNLIV","WHIRLPOOL","WIPRO",
    "WOCKPHARMA","YESBANK","ZEEL","ZENSARTECH","ZENTEC","ZFCVINDIA","ZYDUSLIFE",
    "ZYDUSWELL","BAJAJ-AUTO","COCHINSHIP","CONCORDBIO","CRAFTSMAN","DEEPAKFERT",
]

def get_nse_symbols() -> list[str]:
    """Return deduplicated symbol list."""
    symbols = sorted(set(NIFTY500_SYMBOLS))
    log.info(f"Using {len(symbols)} curated NSE symbols")
    return symbols


# ─── DATA: jugaad-data ────────────────────────────────────────────────────────
def fetch_jugaad(symbol: str) -> Optional[pd.DataFrame]:
    """
    jugaad-data: purpose-built NSE historical data library.
    Handles cookies and sessions correctly. Works on GitHub Actions.
    """
    try:
        from jugaad_data.nse import stock_df
        end   = date.today()
        start = end - timedelta(days=LOOKBACK_DAYS + 60)
        df = stock_df(symbol=symbol, from_date=start, to_date=end, series="EQ")
        if df is None or df.empty:
            return None
        # jugaad returns columns: DATE, SERIES, OPEN, HIGH, LOW, CLOSE, ...
        col_map = {
            'DATE':'Date','OPEN':'Open','HIGH':'High',
            'LOW':'Low','CLOSE':'Close','VOLUME':'Volume',
            'CH_TIMESTAMP':'Date','CH_OPENING_PRICE':'Open',
            'CH_TRADE_HIGH_PRICE':'High','CH_TRADE_LOW_PRICE':'Low',
            'CH_CLOSING_PRICE':'Close','CH_TOT_TRADED_QTY':'Volume',
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        # Lowercase fallback
        df.columns = [c.strip().title() if c.strip().upper() in
                      ['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME']
                      else c for c in df.columns]
        needed = ['Open','High','Low','Close']
        if not all(c in df.columns for c in needed):
            return None
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
        if 'Volume' not in df.columns:
            df['Volume'] = 0
        df = df[['Open','High','Low','Close','Volume']].apply(
            pd.to_numeric, errors='coerce').dropna()
        df = df[df['Close'] > 0].sort_index()
        return df if len(df) >= 30 else None
    except Exception as e:
        log.debug(f"jugaad {symbol}: {e}")
        return None


# ─── DATA: yfinance fallback ──────────────────────────────────────────────────
def fetch_yfinance(symbol: str) -> Optional[pd.DataFrame]:
    """yfinance as fallback — works for many symbols even with rate limits."""
    try:
        import yfinance as yf
        time.sleep(random.uniform(1.5, 3.0))   # aggressive throttle
        ticker = yf.Ticker(f"{symbol}.NS")
        df = ticker.history(period="1y", interval="1d",
                            auto_adjust=True, timeout=30)
        if df is None or df.empty or len(df) < 30:
            return None
        df = df[['Open','High','Low','Close','Volume']].dropna()
        df = df[df['Close'] > 0]
        return df if len(df) >= 30 else None
    except Exception as e:
        log.debug(f"yfinance {symbol}: {e}")
        return None


# ─── DATA: NSE Bhavcopy CSV (daily bulk file) ─────────────────────────────────
_bhavcopy_cache: dict[str, pd.DataFrame] = {}

def fetch_bhavcopy_bulk() -> dict[str, pd.DataFrame]:
    """
    Download NSE bhavcopy (end-of-day bulk CSV) for the last ~1 year.
    One download covers ALL symbols — very efficient.
    Returns {symbol: single_row_df} — only today's OHLCV.
    """
    global _bhavcopy_cache
    if _bhavcopy_cache:
        return _bhavcopy_cache

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
        "Referer": "https://www.nseindia.com/",
    })

    # Try last 5 trading days
    result = {}
    for days_back in range(0, 7):
        d = date.today() - timedelta(days=days_back)
        if d.weekday() >= 5:   # skip weekends
            continue
        date_str = d.strftime("%d%b%Y").upper()
        url = f"https://archives.nseindia.com/content/historical/EQUITIES/{d.year}/{d.strftime('%b').upper()}/cm{date_str}bhav.csv.zip"
        try:
            r = session.get(url, timeout=20)
            if r.status_code != 200:
                continue
            import zipfile
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                fname = z.namelist()[0]
                df = pd.read_csv(z.open(fname))
            df = df[df['SERIES'] == 'EQ']
            for _, row in df.iterrows():
                sym = str(row.get('SYMBOL','')).strip()
                if not sym: continue
                try:
                    result[sym] = {
                        'Open':  float(row['OPEN']),
                        'High':  float(row['HIGH']),
                        'Low':   float(row['LOW']),
                        'Close': float(row['CLOSE']),
                        'Volume':float(row.get('TOTTRDQTY', 0)),
                    }
                except: continue
            if result:
                log.info(f"Bhavcopy loaded {len(result)} symbols from {date_str}")
                break
        except Exception as e:
            log.debug(f"Bhavcopy {date_str}: {e}")
            continue

    _bhavcopy_cache = result
    return result


# ─── Unified Fetch ────────────────────────────────────────────────────────────
def fetch_symbol(symbol: str) -> tuple[str, Optional[pd.DataFrame]]:
    """Try jugaad → yfinance in order."""

    # Source 1: jugaad-data
    df = fetch_jugaad(symbol)
    if df is not None:
        log.debug(f"{symbol}: jugaad OK ({len(df)} bars)")
        return symbol, df

    # Source 2: yfinance (throttled)
    df = fetch_yfinance(symbol)
    if df is not None:
        log.debug(f"{symbol}: yfinance OK ({len(df)} bars)")
        return symbol, df

    log.warning(f"{symbol}: all sources failed")
    return symbol, None


def fetch_all_ohlc(symbols: list[str]) -> dict[str, pd.DataFrame]:
    log.info(f"Fetching {len(symbols)} symbols (workers={MAX_WORKERS})...")
    all_data, failed = {}, []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_symbol, s): s for s in symbols}
        done = 0
        for fut in as_completed(futures):
            sym, df = fut.result()
            done += 1
            if df is not None:
                all_data[sym] = df
            else:
                failed.append(sym)
            if done % 50 == 0 or done == len(symbols):
                log.info(f"  {done}/{len(symbols)} done — "
                         f"{len(all_data)} OK, {len(failed)} failed")

    log.info(f"Download complete: {len(all_data)}/{len(symbols)}")
    if failed[:10]:
        log.info(f"Sample failures: {failed[:10]}")
    return all_data


# ─── Fibonacci Patterns ───────────────────────────────────────────────────────
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
    symbol:str; pattern:str; direction:str; price:float
    X:float; A:float; B:float; C:float; D:float
    x_idx:int; a_idx:int; b_idx:int; c_idx:int; d_idx:int
    prz_low:float=0.0; prz_high:float=0.0
    detected_at:str=field(default_factory=lambda:datetime.utcnow().isoformat())

def fib_ratio(a,b,c): return 0.0 if abs(c-a)<1e-9 else abs(b-a)/abs(c-a)
def in_range(v,lo,hi,tol=FIB_TOLERANCE): return True if lo is None else lo*(1-tol)<=v<=hi*(1+tol)
def compute_prz(X,A,B,C,pat,bull):
    try:
        lo,hi = PATTERNS[pat].get("XD",(None,None))
        if lo is None: return C*0.99,C*1.01
        m=abs(A-X)
        return (sorted([A-m*hi,A-m*lo]) if bull else sorted([A+m*lo,A+m*hi]))
    except: return C*0.98,C*1.02

def zigzag_pivots(df,depth=ZIGZAG_DEPTH):
    H,L,n=df['High'].values,df['Low'].values,len(df['High'])
    if n<depth*2: return []
    pivots,last=[],None
    for i in range(depth,n-depth):
        ih=all(H[i]>=H[i-j] for j in range(1,depth+1)) and all(H[i]>=H[i+j] for j in range(1,depth+1))
        il=all(L[i]<=L[i-j] for j in range(1,depth+1)) and all(L[i]<=L[i+j] for j in range(1,depth+1))
        if ih and last!='H':
            if pivots and last=='H' and H[i]>pivots[-1][1]: pivots[-1]=(i,H[i])
            else: pivots.append((i,H[i]))
            last='H'
        elif il and last!='L':
            if pivots and last=='L' and L[i]<pivots[-1][1]: pivots[-1]=(i,L[i])
            else: pivots.append((i,L[i]))
            last='L'
    return pivots

def match_xabcd(pivots,pattern):
    results,n,r=[],len(pivots),PATTERNS[pattern]
    if n<5: return results
    for i in range(n-4):
        (Xi,X),(Ai,A),(Bi,B),(Ci,C),(Di,D)=pivots[i:i+5]
        bull=A>X
        if pattern=="ABCD":
            if not in_range(fib_ratio(X,B,A),*r["BC"]): continue
            if not in_range(abs(C-B)/(abs(A-X)+1e-9),*r["CD"]): continue
            results.append(HarmonicResult("",pattern,"Bullish" if X<A else "Bearish",
                C,X,A,B,C,C,Xi,Ai,Bi,Ci,Ci,C*0.99,C*1.01)); continue
        if not in_range(fib_ratio(X,B,A),*r["XB"]): continue
        if not in_range(fib_ratio(A,C,B),*r["AC"]): continue
        if not in_range(abs(D-B)/(abs(C-B)+1e-9),*r["BD"]): continue
        if not in_range(fib_ratio(X,D,A),*r["XD"]): continue
        pl,ph=compute_prz(X,A,B,C,pattern,bull)
        if not (pl*0.97<=D<=ph*1.03): continue
        results.append(HarmonicResult("",pattern,"Bullish" if bull else "Bearish",
            D,X,A,B,C,D,Xi,Ai,Bi,Ci,Di,pl,ph))
    return results

def scan_symbol(symbol,df):
    try:
        pivots=zigzag_pivots(df)
        if len(pivots)<5: return []
        last=len(df)-1
        hits=[]
        for pat in PATTERNS:
            for m in match_xabcd(pivots,pat):
                m.symbol=symbol
                if last-m.d_idx<=3: hits.append(m)
        return hits
    except Exception as e:
        log.debug(f"scan {symbol}: {e}"); return []


# ─── Chart ────────────────────────────────────────────────────────────────────
STYLE_TV=mpf.make_mpf_style(base_mpf_style='nightclouds',
    marketcolors=mpf.make_marketcolors(up='#26a69a',down='#ef5350',
    edge='inherit',wick='inherit',volume='in'),
    facecolor='#131722',figcolor='#131722',gridcolor='#1e2130',gridstyle='-')

def generate_chart(symbol,df,r):
    tail=80; df_p=df.iloc[-tail:].copy(); off=len(df)-tail
    pts={k:(max(0,min(v[0]-off,tail-1)),v[1]) for k,v in {
        'X':(r.x_idx,r.X),'A':(r.a_idx,r.A),'B':(r.b_idx,r.B),
        'C':(r.c_idx,r.C),'D':(r.d_idx,r.D)}.items()}
    fig,axes=mpf.plot(df_p,type='candle',style=STYLE_TV,
        title=f' {symbol}  |  {r.pattern}  {r.direction}',
        volume=True,returnfig=True,figsize=(12,7),tight_layout=True)
    ax=axes[0]
    ax.plot([pts[p][0] for p in 'XABCD'],[pts[p][1] for p in 'XABCD'],
        'o-',color='#f0b90b',linewidth=1.5,markersize=4,zorder=5)
    for lbl,(ix,pr) in pts.items():
        ax.annotate(lbl,xy=(ix,pr),xytext=(0,10 if lbl in 'AC' else -15),
            textcoords='offset points',color='#f0b90b',fontsize=9,fontweight='bold',ha='center')
    ax.axhspan(r.prz_low,r.prz_high,alpha=0.15,
        color='#26a69a' if r.direction=='Bullish' else '#ef5350')
    buf=io.BytesIO()
    fig.savefig(buf,format='png',dpi=130,bbox_inches='tight',facecolor='#131722',edgecolor='none')
    plt.close(fig); buf.seek(0); return buf.read()


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram_photo(img,caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return False
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    for _ in range(3):
        try:
            r=requests.post(url,data={'chat_id':TELEGRAM_CHAT_ID,'caption':caption,
                'parse_mode':'HTML'},files={'photo':('chart.png',img,'image/png')},timeout=30)
            if r.status_code==200: return True
        except: pass
        time.sleep(3)
    return False

def telegram_caption(r):
    arrow='🟢' if r.direction=='Bullish' else '🔴'
    return (f"{arrow} <b>{r.pattern}</b> — {r.direction}\n"
            f"📌 <a href='{TV_BASE}{r.symbol}'>{r.symbol}</a>\n"
            f"💰 Price: <b>&#8377;{r.price:.2f}</b>\n"
            f"📊 PRZ: &#8377;{r.prz_low:.2f} – &#8377;{r.prz_high:.2f}")


# ─── Google Sheets ────────────────────────────────────────────────────────────
def get_gsheet():
    if not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID: return None
    try:
        creds=ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(GOOGLE_CREDS_JSON),
            ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive'])
        sh=gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try: ws=sh.worksheet("Alerts")
        except:
            ws=sh.add_worksheet("Alerts",1000,10)
            ws.append_row(["Date","Symbol","Pattern","Direction","Price","PRZ Low","PRZ High","TV Link"])
        return ws
    except Exception as e:
        log.error(f"Sheets: {e}"); return None

def append_gsheet(ws,results):
    if not ws or not results: return
    today=date.today().isoformat()
    rows=[[today,r.symbol,r.pattern,r.direction,round(r.price,2),
           round(r.prz_low,2),round(r.prz_high,2),f"{TV_BASE}{r.symbol}"] for r in results]
    try: ws.append_rows(rows,value_input_option='USER_ENTERED')
    except Exception as e: log.error(f"Sheets append: {e}")


# ─── Email ────────────────────────────────────────────────────────────────────
def send_email(total,results):
    if not EMAIL_FROM or not EMAIL_TO: return
    today=date.today().strftime("%d %b %Y")
    row_html=""
    for r in results:
        clr='#26a69a' if r.direction=='Bullish' else '#ef5350'
        row_html+=(f"<tr><td><a href='{TV_BASE}{r.symbol}' style='color:#f0b90b'>{r.symbol}</a></td>"
                   f"<td>{r.pattern}</td><td style='color:{clr}'>{r.direction}</td>"
                   f"<td>&#8377;{r.price:.2f}</td><td>&#8377;{r.prz_low:.2f}–{r.prz_high:.2f}</td></tr>")
    html=(f"<html><body style='background:#131722;color:#d1d4dc;font-family:Arial;padding:20px'>"
          f"<h2 style='color:#f0b90b'>NSE Harmonic Scanner — {today}</h2>"
          f"<p>Scanned:<b>{total}</b> | Detected:<b>{len(results)}</b></p>"
          f"<table cellpadding='8' style='border-collapse:collapse;width:100%;background:#1e2130'>"
          f"<tr style='background:#2a2e3f;color:#f0b90b'>"
          f"<th>Symbol</th><th>Pattern</th><th>Direction</th><th>Price</th><th>PRZ</th></tr>"
          f"{row_html}</table>"
          f"<p style='color:#555;font-size:11px'>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>"
          f"</body></html>")
    msg=MIMEMultipart('alternative')
    msg['Subject']=f"NSE Harmonic Scanner — {today} | {len(results)} Alerts"
    msg['From']=EMAIL_FROM; msg['To']=EMAIL_TO
    msg.attach(MIMEText(html,'html'))
    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST,EMAIL_SMTP_PORT,timeout=30) as s:
            s.ehlo(); s.starttls(); s.login(EMAIL_FROM,EMAIL_PASSWORD)
            s.sendmail(EMAIL_FROM,EMAIL_TO,msg.as_string())
        log.info("Email sent.")
    except Exception as e: log.error(f"Email: {e}")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run_scanner():
    t0=time.time()
    log.info("="*60)
    log.info(f"NSE Harmonic Scanner v4 — {datetime.utcnow().isoformat()}")
    log.info("Data: jugaad-data (primary) → yfinance (fallback)")

    symbols=get_nse_symbols()
    ohlc=fetch_all_ohlc(symbols)

    if not ohlc:
        log.error("0 symbols downloaded.")
        send_email(0,[])
        return

    all_results,ws=[],get_gsheet()
    for sym,df in ohlc.items():
        hits=scan_symbol(sym,df)
        if not hits: continue
        log.info(f"  [{sym}] {len(hits)} pattern(s)")
        for r in hits:
            all_results.append(r)
            try: send_telegram_photo(generate_chart(sym,df,r),telegram_caption(r))
            except Exception as e: log.error(f"Chart/TG {sym}: {e}")
            time.sleep(0.3)

    if all_results: append_gsheet(ws,all_results)
    send_email(len(ohlc),all_results)

    log.info(f"Done — {len(all_results)} patterns / {len(ohlc)} symbols / {time.time()-t0:.1f}s")
    with open(f"results_{date.today()}.json",'w') as f:
        json.dump([asdict(r) for r in all_results],f,indent=2)

if __name__=="__main__":
    run_scanner()
