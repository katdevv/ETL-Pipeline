import os, requests, json
from dotenv import load_dotenv
from pathlib import Path
from datetime import date
import pandas as pd

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["AAPL", "GOOG", "MSFT"]

PATH = Path('data/raw_data')
PATH.mkdir(parents=True, exist_ok=True)

def extract(symbol: str, overwrite: bool = False) -> Path:
    # Download today's daily time series for a symbol and save it as raw JSON

    # Skip if already exist for today
    # Overwrite=True to force
    filename = PATH / f"{symbol}_{date.today()}.json"
    if filename.exists() and not overwrite:
        print(f"{filename.name} already exists. Use overwrite=True to force")
        return filename

    # Fetch stock data
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Write it into JSON file
    with filename.open("w") as f:
        json.dump(data, f, indent=2)

    return filename

def _latest_file_for(symbol: str) -> Path:
    # Pick the newest raw JSON for the symbol
    files = sorted(PATH.glob(f"{symbol}_*.json"))
    if not files:
        raise FileNotFoundError(f"No json files for {symbol}")
    return files[-1]

def parse_json(path: Path, symbol: str) -> pd.DataFrame:
    # Read the raw JSON into a pandas DataFrame
    with path.open("r") as f:
        payload = json.load(f)

    timeSeries = payload.get("Time Series (Daily)")
    if timeSeries is None:
        raise ValueError(f"{symbol}: missing Time Series in {path.name}")

    # Clean and organize the data into columns
    rows = []
    for d, v in timeSeries.items():
        rows.append({
            "symbol": symbol,
            "date": pd.to_datetime(d),
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low":  float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": int(v["5. volume"]),
        })

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    # Add a new column daily_change_percentage
    df["daily_change_percentage"] = (df["close"] - df["open"]) / df["open"] * 100
    return df

def transform(symbols = SYMBOLS) -> pd.DataFrame:
    # Load the latest raw JSON for each symbol, parse, then concatenate
    frames = [] # For each symbol
    for s in symbols:
        raw_path = _latest_file_for(s)
        frames.append(parse_json(raw_path, s))

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    return combined

if __name__ == "__main__":
    # Extract
    for s in SYMBOLS:
        extract(s)

    # Transform
    combined_df = transform(SYMBOLS)

    print(combined_df.sample(10))





