import os, requests, json, sqlite3
from dotenv import load_dotenv
from pathlib import Path
from datetime import date, datetime, UTC
import pandas as pd

# validation + scheduler
from typing import Dict
from pydantic import BaseModel, Field, ValidationError

# Load environment variables
load_dotenv()

# Alpha vantage API
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["AAPL", "GOOG", "MSFT"]

# Make directory for JSON files
PATH = Path('data/raw_data')
PATH.mkdir(parents=True, exist_ok=True)

# Make directory for SQL databases
DB = Path("data/db")
DB.mkdir(parents=True, exist_ok=True)
DB_PATH = DB / "stocks.sqlite"

# Pydantic models
class _DailyBar(BaseModel):
    # validate API shape
    open: str  = Field(alias="1. open")
    high: str  = Field(alias="2. high")
    low:  str  = Field(alias="3. low")
    close: str = Field(alias="4. close")
    volume: str = Field(alias="5. volume")

class _Payload(BaseModel):
    ts: Dict[str, _DailyBar] = Field(alias="Time Series (Daily)")

def _validate_payload(payload: dict, symbol: str) -> Dict[str, _DailyBar]:
    try:
        return _Payload.model_validate(payload).ts  # pydantic v2
    except AttributeError:
        return _Payload.parse_obj(payload).ts        # pydantic v1
    except ValidationError as e:
        raise ValueError(f"{symbol}: invalid API payload: {e}")

# Extract from API
def extract(symbol: str, overwrite: bool = False) -> Path:
    filename = PATH / f"{symbol}_{date.today()}.json"
    if filename.exists() and not overwrite:
        print(f"{filename.name} already exists. Use overwrite=True to force")
        return filename

    params = {"function": "TIME_SERIES_DAILY","symbol": symbol,"apikey": API_KEY}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    with filename.open("w") as f:
        json.dump(data, f, indent=2)
    return filename

def _latest_file_for(symbol: str) -> Path:
    files = sorted(PATH.glob(f"{symbol}_*.json"))
    if not files:
        raise FileNotFoundError(f"No json files for {symbol}")
    return files[-1]

def parse_json(path: Path, symbol: str) -> pd.DataFrame:
    with path.open("r") as f:
        payload = json.load(f)

    # use validator (minimal change)
    timeSeries = _validate_payload(payload, symbol)

    rows = []
    for d, v in timeSeries.items():
        rows.append({
            "symbol": symbol,
            "date": pd.to_datetime(d),
            "open": float(v.open),
            "high": float(v.high),
            "low":  float(v.low),
            "close": float(v.close),
            "volume": int(v.volume),
        })
    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    df["daily_change_percentage"] = (df["close"] - df["open"]) / df["open"] * 100
    return df

# Transform in DataFrames
def transform(symbols = SYMBOLS) -> pd.DataFrame:
    frames = []
    for s in symbols:
        raw_path = _latest_file_for(s)
        frames.append(parse_json(raw_path, s))
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    return combined

# Load in SQLite
def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,                       -- YYYY-MM-DD
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL,
            daily_change_percentage REAL NOT NULL,
            extraction_timestamp TEXT NOT NULL,       -- UTC ISO
            UNIQUE(symbol, date)
        );
    """)
    conn.commit()

def load(df: pd.DataFrame):
    if df.empty:
        print("DF empty, nothing to write")
        return 0
    out = df.copy()
    out.rename(columns={"open":"open_price","high":"high_price","low":"low_price","close":"close_price"}, inplace=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    out["extraction_timestamp"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
    cols = ["symbol","date","open_price","high_price","low_price","close_price","volume","daily_change_percentage","extraction_timestamp"]

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        sql = """
            INSERT INTO stock_daily_data
            (symbol,date,open_price,high_price,low_price,close_price,volume,daily_change_percentage,extraction_timestamp)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol,date) DO UPDATE SET
                open_price=excluded.open_price,
                high_price=excluded.high_price,
                low_price=excluded.low_price,
                close_price=excluded.close_price,
                volume=excluded.volume,
                daily_change_percentage=excluded.daily_change_percentage,
                extraction_timestamp=excluded.extraction_timestamp
        """
        conn.executemany(sql, out[cols].itertuples(index=False, name=None))
        conn.commit()

# Python scheduler
def run_once():
    for s in SYMBOLS: extract(s)
    load(transform(SYMBOLS))

def run_daily_with_schedule(at="18:30"):
    import time, schedule
    schedule.every().day.at(at).do(run_once)
    print(f"scheduled daily at {at}")
    while True:
        schedule.run_pending(); time.sleep(30)

if __name__ == "__main__":
    # run_once() # commented by default, run immediately
    for s in SYMBOLS: extract(s) # or run manually
    combined_df = transform(SYMBOLS)
    load(combined_df)

    print(combined_df.sample(10))
