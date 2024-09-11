import ccxt
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import os
import dropbox

# Configurare exchange și simbol
exchange = ccxt.bybit({
    'proxies': {
        'http': '130.61.171.71:80',
        'http': '116.203.28.43:80',
        'http': '188.40.59.208:80',
        'http': '202.61.206.250:8888',
        'http': '94.130.94.45:80',
        'http': '51.254.78.223:80',
    },
    'rateLimit': 1000,
    'enableRateLimit': True,
})
symbol = 'WEMIX/USDT'

# Token de autentificare Dropbox
DROPBOX_TOKEN = os.environ.get('token')
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# Functie pentru a genera intervalele de valori pentru procente
def frange(start, stop, step):
    while start <= stop:
        yield round(start, 3)
        start += step

# Intervalele pentru procente
signal_percent_range = [round(x, 3) for x in frange(0.014, 0.04, 0.002)]
profit_percent_range = [round(x, 3) for x in frange(0.01, 0.04, 0.002)]
stop_loss_percent_range = [round(x, 2) for x in frange(0.15, 0.3, 0.01)]

# Funcție pentru încărcarea fișierului pe Dropbox
def upload_to_dropbox(local_path, dropbox_path):
    with open(local_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode("overwrite"))

# Funcție pentru descărcarea lumânărilor de 30 de minute
def download_candles(timeframe, since):
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)

# Descărcarea tuturor lumânărilor de 30 de minute folosind threading
def download_all_data(start_date, end_date):
    start_timestamp = exchange.parse8601(start_date + 'T00:00:00Z')
    end_timestamp = exchange.parse8601(end_date + 'T00:00:00Z')

    all_candles = []
    current_timestamp = start_timestamp

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        while current_timestamp < end_timestamp:
            futures.append(executor.submit(download_candles, '30m', current_timestamp))
            current_timestamp += 1000 * 30 * 60 * 1000  # Incrementarea cu numărul de lumânări * 30m

        for future in as_completed(futures):
            all_candles.extend(future.result())

    # Convertirea datelor în DataFrame pentru prelucrare ulterioară
    df_1h = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df_1h['timestamp'] = pd.to_datetime(df_1h['timestamp'], unit='ms')

    return df_1h

# Cache pentru datele de 1 minut
minute_data_cache = {}

# Funcție pentru descărcarea și stocarea datelor de 1 minut în cache
def get_minute_data(start_time):
    if start_time in minute_data_cache:
        return minute_data_cache[start_time]

    minute_candles = exchange.fetch_ohlcv(symbol, timeframe='1m', since=int(start_time.timestamp() * 1000), limit=30)
    df_1m = pd.DataFrame(minute_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], unit='ms')
    minute_data_cache[start_time] = df_1m

    return df_1m

# Funcția de procesare a tranzacțiilor folosind multiprocessing
def process_trades(df_1h, signal_percent, profit_percent, stop_loss_percent):
    buy_limit_percent = 1 - signal_percent
    sell_limit_percent = 1 + signal_percent
    profitable_trades = 0
    unprofitable_trades = 0
    total_profitable_value = 0
    total_unprofitable_value = 0

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
            df_1m = get_minute_data(open_timestamp)

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

            # Dacă tranzacția nu s-a închis în lumânările de 1 minut, continuă cu cele de 30 de minute
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

    difference = total_profitable_value - total_unprofitable_value
    difference_sign = "+" if difference > 0 else "-"

    return (signal_percent, profit_percent, stop_loss_percent, profitable_trades, unprofitable_trades, f"{difference_sign}{abs(difference):.2f}")

# Funcția de execuție a tranzacțiilor cu multiprocessing
def execute_trades(df_1h):
    results_file = 'trade_results.csv'
    dropbox_file_path = '/trade_results.csv'

    with open(results_file, 'w') as file:
        file.write('signal_percent,profit_percent,stop_loss_percent,profitable_trades,unprofitable_trades,difference\n')

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for signal_percent in signal_percent_range:
            for profit_percent in profit_percent_range:
                for stop_loss_percent in stop_loss_percent_range:
                    futures.append(executor.submit(process_trades, df_1h, signal_percent, profit_percent, stop_loss_percent))

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            signal_percent, profit_percent, stop_loss_percent, profitable_trades, unprofitable_trades, difference = result
            # Salvarea rezultatelor în fișier
            with open(results_file, 'a') as file:
                file.write(f'{signal_percent},{profit_percent},{stop_loss_percent},{profitable_trades},{unprofitable_trades},{difference}\n')

            # Încărcarea imediată pe Dropbox după fiecare scriere
            upload_to_dropbox(results_file, dropbox_file_path)

if __name__ == '__main__':
    # Parametri de timp pentru descărcarea datelor
    start_date = '2023-12-01'
    end_date = '2024-08-30'

    # Rulează descărcarea și procesarea datelor
    df_1h = download_all_data(start_date, end_date)
    execute_trades(df_1h)

    print("Finalizat")