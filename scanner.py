import pandas as pd
import requests
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import schedule

# ===== CONFIG =====
CSV_FILE = "buy_signals.csv"
OPEN_TRADES_FILE = "open_trades.csv"
EMA_SHORT = 50
EMA_MED = 100
EMA_LONG = 200
EMA_TOLERANCE = 0.05
STRONG_GREEN_PCT = 0.005
MIN_CANDLES = 60
MAX_THREADS = 10
BATCH_DELAY = 0.1

# Telegram credentials
TELEGRAM_TOKEN = "7714134495:AAHbFQujX8GEcPAkLg0c6_h4DDt1wzcWadc"
CHAT_ID = "541238511"


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# ===== HELPER FUNCTIONS =====
def send_telegram(msg):
    msg = msg.replace("_","\\_").replace("*","\\*").replace("[","\\[")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "MarkdownV2"})
        if resp.status_code != 200:
            print(f"Telegram failed: {resp.text}")
    except Exception as e:
        print(f"Telegram error: {e}")

def get_nifty500():
    df = pd.read_csv("https://archives.nseindia.com/content/indices/ind_nifty500list.csv")
    df["Symbol"] = df["Symbol"].astype(str)
    return df["Symbol"].tolist()

def download_nse_stock(symbol, from_date="01-01-2023", to_date=None):
    if to_date is None:
        to_date = datetime.today().strftime("%d-%m-%Y")
    url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=[EQ]&from={from_date}&to={to_date}"
    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=HEADERS)
        response = session.get(url, headers=HEADERS)
        data = response.json()
        if "data" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["data"])
        df["open"] = pd.to_numeric(df["open"], errors='coerce')
        df["high"] = pd.to_numeric(df["high"], errors='coerce')
        df["low"] = pd.to_numeric(df["low"], errors='coerce')
        df["close"] = pd.to_numeric(df["close"], errors='coerce')
        df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y")
        return df.sort_values("date")
    except:
        return pd.DataFrame()

def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def atr(high, low, close, period=14):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def save_to_csv(signals, filename):
    if not os.path.exists(filename):
        pd.DataFrame(columns=["Stock","Price","Strategy","SL","Target1","Target2","Target3","Date"]).to_csv(filename,index=False)
    df = pd.read_csv(filename)
    df = pd.concat([df, pd.DataFrame(signals)], ignore_index=True)
    df.to_csv(filename,index=False)

# ===== SIGNAL LOGIC =====
def check_signals(stock, df, strategy_name="EMA50 Breakout"):
    signals = []
    if df.empty or len(df) < MIN_CANDLES:
        return []

    df["EMA50"] = ema(df["close"], EMA_SHORT)
    df["EMA100"] = ema(df["close"], EMA_MED)
    df["EMA200"] = ema(df["close"], EMA_LONG)
    df["ATR"] = atr(df["high"], df["low"], df["close"])

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    trend_ok = (
        latest["EMA50"] > latest["EMA100"] and
        latest["EMA100"] > latest["EMA200"] and
        (latest["EMA50"] - df["EMA50"].iloc[-5])/df["EMA50"].iloc[-5] > 0
    )

    near_ema50 = prev["close"] >= latest["EMA50"]*(1-EMA_TOLERANCE) and prev["close"] <= latest["EMA50"]*(1+EMA_TOLERANCE)
    strong_green = (latest["close"] > latest["open"]) and ((latest["close"] - latest["open"])/latest["open"] > STRONG_GREEN_PCT)

    if trend_ok and near_ema50 and strong_green:
        sl = df["low"][-5:].min()
        targets = [latest["close"] + latest["ATR"], latest["close"] + 2*latest["ATR"], latest["close"] + 3*latest["ATR"]]
        signal = {
            "Stock": stock,
            "Price": latest["close"],
            "Strategy": strategy_name,
            "SL": sl,
            "Target1": targets[0],
            "Target2": targets[1],
            "Target3": targets[2],
            "Date": datetime.now()
        }
        signals.append(signal)
        msg = f"{stock} - {strategy_name} | Price: {latest['close']:.2f} | SL: {sl:.2f} | Targets: {targets[0]:.2f},{targets[1]:.2f},{targets[2]:.2f}"
        print(msg)
        send_telegram(msg)
    return signals

# ===== INTRADAY / DAILY SCAN WORKER =====
def process_stock(stock):
    all_signals = []
    try:
        # Daily EMA50
        df_daily = download_nse_stock(stock)
        signals_daily = check_signals(stock, df_daily, "EMA50 Daily")
        all_signals.extend(signals_daily)

        # Intraday placeholder (15-min)
        df_15min = df_daily  # Replace with real intraday API if available
        signals_15min = check_signals(stock, df_15min, "EMA50 15-min")
        all_signals.extend(signals_15min)

        # Intraday placeholder (1-hour)
        df_1h = df_daily  # Replace with real intraday API if available
        signals_1h = check_signals(stock, df_1h, "EMA50 1h")
        all_signals.extend(signals_1h)

        time.sleep(BATCH_DELAY)
    except Exception as e:
        print(f"Error processing {stock}: {e}")
    return all_signals

# ===== MONITOR OPEN TRADES =====
def monitor_trades():
    if not os.path.exists(OPEN_TRADES_FILE):
        return
    df = pd.read_csv(OPEN_TRADES_FILE)
    if df.empty:
        return
    updated = []
    for _, row in df.iterrows():
        df_stock = download_nse_stock(row["Stock"], from_date=datetime.today().strftime("%d-%m-%Y"))
        if df_stock.empty:
            updated.append(row)
            continue
        latest = df_stock["close"].iloc[-1]
        sl = row["SL"]
        targets = [row["Target1"], row["Target2"], row["Target3"]]

        if latest >= targets[2]:
            send_telegram(f"🎯 {row['Stock']} hit Target3: {latest:.2f}")
        elif latest >= targets[1]:
            send_telegram(f"🎯 {row['Stock']} hit Target2: {latest:.2f}")
        elif latest >= targets[0]:
            send_telegram(f"🎯 {row['Stock']} hit Target1: {latest:.2f}")
        elif latest <= sl:
            send_telegram(f"❌ {row['Stock']} hit Stop-Loss: {latest:.2f}")
        else:
            updated.append(row)
    pd.DataFrame(updated).to_csv(OPEN_TRADES_FILE,index=False)

# ===== MAIN SCAN FUNCTION =====
def run_scanner(manual=False):
    print(f"\n=== Scanner running at {datetime.now()} ===")
    stocks = get_nifty500()
    all_signals = []
    total_stocks = len(stocks)
    print(f"Total stocks to scan: {total_stocks}")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(process_stock, stock): stock for stock in stocks}
        for i, future in enumerate(as_completed(future_to_stock), start=1):
            stock_signals = future.result()
            all_signals.extend(stock_signals)
            print(f"[{i}/{total_stocks}] Scanned {future_to_stock[future]} - Signals found: {len(stock_signals)}")

    if all_signals:
        save_to_csv(all_signals, CSV_FILE)
        save_to_csv(all_signals, OPEN_TRADES_FILE)
        print(f"✅ Total signals found: {len(all_signals)}")
    else:
        print("No signals found today.")

    monitor_trades()
    print(f"Scanner completed at {datetime.now()}")
    if manual:
        send_telegram("✅ Manual NSE EMA50 scan completed")

# ===== COMMAND-LINE ARGUMENTS =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manual", action="store_true", help="Run scanner immediately")
    args = parser.parse_args()

    if args.manual:
        run_scanner(manual=True)
    else:
        schedule.every().day.at("15:45").do(run_scanner)
        print("✅ Daily NSE EMA50 Multi-threaded Scanner scheduled at 15:45 IST")
        send_telegram("✅ NSE EMA50 Scanner scheduled successfully")
        while True:
            schedule.run_pending()
            time.sleep(60)
