import urllib.request
import datetime, json, sqlite3
import pandas as pd
pd.set_option('display.max_columns', 0)
import pprint
pp = pprint.PrettyPrinter(indent=4)

BASEURL = 'http://localhost:18080/kabusapi/'
BASEURL_TEST = 'http://localhost:18081/kabusapi/'

DB_PATH = '../data/kabustation_stock.db'


## Connect to DB
con = sqlite3.connect(DB_PATH)


## Get TOKEN
from getpass import getpass
password = getpass()

json_data = json.dumps({'APIPassword': password}).encode('utf8')
req = urllib.request.Request(
    BASEURL+'token',
    json_data,
    method='POST'
)
req.add_header('Content-Type', 'application/json')

res = urllib.request.urlopen(req)
print(res.status, res.reason)

content = json.loads(res.read())
token = content['Token']


## Register Brand
obj = { 'Symbols':
        [
            {'Symbol': '4689', 'Exchange': 1},
        ] }
json_data = json.dumps(obj).encode('utf8')

url = 'http://localhost:18080/kabusapi/register'
req = urllib.request.Request(url, json_data, method='PUT')
req.add_header('Content-Type', 'application/json')
req.add_header('X-API-KEY', token)


res = urllib.request.urlopen(req)
print(res.status, res.reason)
#for header in res.getheaders():
#    print(header)

content = json.loads(res.read())
pp.pprint(content)


## Functions
def convertToPlain(dict_raw):
    dict_plain = dict_raw.copy()
    for board_type in ['Buy', 'Sell']:
        for i in range(1, 11):
            key = board_type + str(i)
            board = dict_plain.pop(key, None)
            if i == 1:
                for s in ['Sign', 'Time']:
                    dict_plain[key+'_'+s] = board[s]
            for s in ['Price', 'Qty']:
                dict_plain[key+'_'+s] = board[s]
    return dict_plain

def to_datetime(df):
    for col in df.columns:
        if 'Time' in col:
            df[col] = pd.to_datetime(df[col])
    return df

def to_csv(df, filename):
    if not os.path.isfile(filename):
        df.to_csv(filename)
    else:
        df.to_csv(filename, mode='a', header=False)

# freq = 1s, 1min, 5min, 1h, ...
def groupByTime(df, col, freq=None):
    df[col+'Time'] = pd.to_datetime(df[col+'Time'])
    df_sel = df.loc[:, [col+'Time', col]]
    if freq:
        df_gb = df_sel.groupby(pd.Grouper(key=col+'Time', freq=freq))
    else:
        df_gb = df_sel.groupby(col+'Time')
    return df_gb

def CreateStockData(df, freq=None):
    df_gb = groupByTime(df, 'CurrentPrice', freq=freq)
    df_vol = groupByTime(df, 'TradingVolume', freq=freq).max()
    df_vol = df_vol.diff()
    df_stock = pd.concat([df_gb.first(), df_gb.max(), df_gb.min(), df_gb.last(), df_vol], axis=1)
    df_stock = df_stock.set_axis(['Open', 'High', 'Low', 'Close', 'Volume'], axis='columns')
    return df_stock


## Get PUSH data
import json, sys, time
import websocket
from datetime import datetime

FILENAME_RAW = '../data/' + time.strftime("%Y%m%d") + '_push_raw.csv'
FILENAME_STOCK = '../data/' + time.strftime("%Y%m%d") + '_push_stock.csv'

is_first_data = True

def on_message(ws, message):
    print(datetime.now(), ', --- RECV MSG. ---', end='\r')
    #print(message)

    # String --> dict
    raw_dict = json.loads(message)

    # Convert to plain
    plain_dict = convertToPlain(raw_dict)

    stock_code = plain_dict['Symbol']
    market_code = plain_dict['Exchange']
    table_name = 'tick_' + str(stock_code) + '_' + str(market_code)

    # dict --> dataframe
    tick_raw_df = pd.DataFrame([plain_dict]).iloc[: , 1:]
    tick_raw_df = to_datetime(tick_raw_df)
    #to_csv(tick_raw_df, FILENAME_RAW) # save raw data
    tick_raw_df.to_sql(
        name=table_name, con=con, if_exists='append', index=False
    )

def on_error(ws, error):
    print('--- ERROR --- ')
    print(error)

def on_close(ws):
    print('--- DISCONNECTED --- ')

def on_open(ws):
    print('--- CONNECTED --- ')

url = 'ws://localhost:18080/kabusapi/websocket'
# websocket.enableTrace(True)
ws = websocket.WebSocketApp(
    url,
    on_message = on_message,
    on_error = on_error,
    on_close = on_close
)
ws.on_open = on_open
ws.run_forever()
