import os, requests, json
from dotenv import load_dotenv
from pathlib import Path
from datetime import date

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def extract(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    path = Path('data/raw_data')
    path.mkdir(parents=True, exist_ok=True)

    filename = path / f"{symbol}_{date.today()}.json"
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

    print(filename)

for s in ["AAPL", "GOOG", "MSFT"]:
    extract(s)