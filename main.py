import ccxt
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import dropbox

# Configurare exchange și simbol
exchange = ccxt.binance()
symbol = 'WLD/USDT'

DROPBOX_TOKEN = os.environ.get('token')

dbx = dropbox.Dropbox(DROPBOX_TOKEN)

def frange(start, stop, step):
    while start <= stop:
        yield round(start, 3)  # Rotunjire pentru procente
        start += step

# Setarea perioadei de analiză
start_date = '2024-01-01'
end_date = '2024-08-30'

# Convertirea datelor în timestamp-uri Unix
start_timestamp = exchange.parse8601(start_date + 'T00:00:00Z')
end_timestamp = exchange.parse8601(end_date + 'T00:00:00Z')

# Intervalele pentru procente
signal_percent_range = [round(x, 3) for x in frange(0.014, 0.036, 0.002)]
profit_percent_range = [round(x, 3) for x in frange(0.01, 0.036, 0.002)]
stop_loss_percent_range = [round(x, 2) for x in frange(0.15, 0.30, 0.01)]

# Fișier pentru rezultate
results_file = 'trade_results.csv'
dropbox_file_path = '/trade_results.csv'

# Inițializarea fișierului pentru rezultate
with open(results_file, 'w') as file:
    file.write('signal_percent,profit_percent,stop_loss_percent,profitable_trades,unprofitable_trades,difference\n')

def process_trades(signal_percent, profit_percent, stop_loss_percent):
    buy_limit_percent = 1 - signal_percent
    sell_limit_percent = 1 + signal_percent

    # Listă pentru stocarea tuturor datelor de lumânări
    all_candles = []

    # Descărcarea tuturor lumânărilor folosind o buclă
    current_timestamp = start_timestamp
    while current_timestamp < end_timestamp:
        candles = exchange.fetch_ohlcv(symbol, timeframe='30m', since=current_timestamp, limit=1000)
        if not candles:
            break
        all_candles.extend(candles)
        current_timestamp = candles[-1][0]

    # Convertirea în DataFrame
    df_1h = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df_1h['timestamp'] = pd.to_datetime(df_1h['timestamp'], unit='ms')

    # Inițializarea listelor pentru tranzacțiile profitabile și neprofitabile
    profitable_trades = 0
    unprofitable_trades = 0

    # Suma valorilor tranzacțiilor profitabile și neprofitabile
    total_profitable_value = 0
    total_unprofitable_value = 0

    # Analizarea fiecărei lumânări de o oră
    for i in range(len(df_1h)):
        open_price = df_1h.loc[i, 'open']
        high_price = df_1h.loc[i, 'high']
        low_price = df_1h.loc[i, 'low']
        open_timestamp = df_1h.loc[i, 'timestamp']

        buy_limit = open_price * buy_limit_percent
        sell_limit = open_price * sell_limit_percent

        trade_opened = False
        entry_price = None
        stop_loss = None
        trade_type = None
        close_timestamp = None

        if low_price <= buy_limit:
            trade_opened = True
            entry_price = buy_limit
            stop_loss = buy_limit * (1 - stop_loss_percent)
            trade_type = 'BUY'
        elif high_price >= sell_limit:
            trade_opened = True
            entry_price = sell_limit
            stop_loss = sell_limit * (1 + stop_loss_percent)
            trade_type = 'SELL'

        if trade_opened:
            minute_candles = exchange.fetch_ohlcv(symbol, timeframe='1m', since=int(open_timestamp.timestamp() * 1000), limit=30)
            df_1m = pd.DataFrame(minute_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], unit='ms')

            for j in range(len(df_1m)):
                current_high = df_1m.loc[j, 'high']
                current_low = df_1m.loc[j, 'low']
                minute_timestamp = df_1m.loc[j, 'timestamp']

                if trade_type == 'BUY':
                    if current_low <= entry_price:
                        if current_high >= entry_price * (1 + profit_percent):
                            close_timestamp = minute_timestamp
                            total_profitable_value += profit_percent * 100
                            profitable_trades += 1
                            break
                        elif current_low <= stop_loss:
                            close_timestamp = minute_timestamp
                            total_unprofitable_value += stop_loss_percent * 100
                            unprofitable_trades += 1
                            break

                elif trade_type == 'SELL':
                    if current_high >= entry_price:
                        if current_low <= entry_price * (1 - profit_percent):
                            close_timestamp = minute_timestamp
                            total_profitable_value += profit_percent * 100
                            profitable_trades += 1
                            break
                        elif current_high >= stop_loss:
                            close_timestamp = minute_timestamp
                            total_unprofitable_value += stop_loss_percent * 100
                            unprofitable_trades += 1
                            break

            if close_timestamp is None:
                for k in range(i + 1, len(df_1h)):
                    next_high = df_1h.loc[k, 'high']
                    next_low = df_1h.loc[k, 'low']
                    close_timestamp = df_1h.loc[k, 'timestamp']

                    if trade_type == 'BUY':
                        if next_high >= entry_price * (1 + profit_percent):
                            total_profitable_value += profit_percent * 100
                            profitable_trades += 1
                            break
                        elif next_low <= stop_loss:
                            total_unprofitable_value += stop_loss_percent * 100
                            unprofitable_trades += 1
                            break

                    elif trade_type == 'SELL':
                        if next_low <= entry_price * (1 - profit_percent):
                            total_profitable_value += profit_percent * 100
                            profitable_trades += 1
                            break
                        elif next_high >= stop_loss:
                            total_unprofitable_value += stop_loss_percent * 100
                            unprofitable_trades += 1
                            break

    # Calculul diferenței procentuale dintre tranzacțiile profitabile și neprofitabile
    difference = total_profitable_value - total_unprofitable_value
    difference_sign = "+" if difference > 0 else "-"
    
    return (signal_percent, profit_percent, stop_loss_percent, profitable_trades, unprofitable_trades, f"{difference_sign}{abs(difference):.2f}")

def upload_to_dropbox(local_path, dropbox_path):
    with open(local_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode("overwrite"))

# Utilizarea ThreadPoolExecutor pentru a executa sarcinile în paralel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    for signal_percent in signal_percent_range:
        for profit_percent in profit_percent_range:
            for stop_loss_percent in stop_loss_percent_range:
                futures.append(executor.submit(process_trades, signal_percent, profit_percent, stop_loss_percent))

    # Adăugarea unui progress bar folosind tqdm
    for future in tqdm(as_completed(futures), total=len(futures)):
        result = future.result()
        signal_percent, profit_percent, stop_loss_percent, profitable_trades, unprofitable_trades, difference = result
        # Salvarea rezultatelor în fișier
        with open(results_file, 'a') as file:
            file.write(f'{signal_percent},{profit_percent},{stop_loss_percent},{profitable_trades},{unprofitable_trades},{difference}\n')
        
        # Încărcarea imediată pe Dropbox după fiecare scriere
        upload_to_dropbox(results_file, dropbox_file_path)

print("Finalizat")
