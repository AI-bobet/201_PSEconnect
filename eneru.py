# %%

import numpy as np
from datetime import datetime, timedelta
import os
import pandas as pd
import sqlalchemy
import logging
from tvDatafeed import TvDatafeed, Interval
import time
start_time = time.time()

logging.basicConfig(level=logging.DEBUG)
date_today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# %% load database tables
engine = sqlalchemy.create_engine('postgresql://{0}:{1}@{2}:{3}/trading'.format(os.environ.get('db_user'), os.environ.get('db_password'),
                                                                                os.environ.get('db_host'), os.environ.get('db_port')))

pse_tickers = pd.read_sql_table('portfolio', engine,
                                index_col='ticker').sort_index()
pse_index_tickers = pd.read_sql_table(
    'pse_index_info', engine, index_col='ticker')
db_date = pd.read_sql_table('date', engine, index_col='date').sort_index()
# %% Scrape tradingview data

tv = TvDatafeed()
pse_pricedata = pd.DataFrame()

for ticker in pse_tickers.loc[pse_tickers['exchange_id'] == 1].index:
    try:
        df = tv.get_hist(ticker[4:], 'PSE', n_bars=10000)
        df.rename(columns={'symbol': 'ticker'}, inplace=True)
        df.index = df.index.strftime("%Y-%m-%d")
        df.index = pd.to_datetime(df.index)
        df.index.name = 'date'
        if ticker in pse_index_tickers.index:
            df.replace(df['volume'][0], 0, inplace=True)
        pse_pricedata = pd.concat([pse_pricedata, df])
    except AttributeError:
        # pass
        print("No available price data for {0}".format(ticker))

# %% Date Table

# Check if historical price data's date already exist in the database
db_date_inputs = pd.DataFrame(np.setdiff1d(
    pse_pricedata.index, db_date.index), columns=['date'])

if db_date_inputs.empty == False:
    for date in db_date_inputs['date'].to_list():
        print("{0} has been added to the trading db's DATE TABLE".format(
            datetime.strftime(date, "%Y-%m-%d")))
        print('')
        print("Date: {0}".format(date_today))
        # input the date to the database
        db_date_inputs.to_sql(name='date', con=engine,
                              index=False, if_exists='append')

# reload datebase's date table
db_date = pd.read_sql_table('date', engine, index_col='date').sort_index()

# transform all the date and items to iterable usig map() method
date_map = dict(zip(db_date.index, db_date['date_id']))
ticker_map = dict(zip(pse_tickers.index, pse_tickers['ticker_id']))

# %% PSE historical Price Data

# Check if OHLCV data if alrady exist in the DB
db_ohlcv = pd.read_sql_table('ohlcv', engine)
db_ohlcv = db_ohlcv.loc[db_ohlcv['exchange_id'] == 1]
# %%
pse_pricedata.reset_index(inplace=True)
pse_pricedata['date'] = pse_pricedata['date'].map(date_map)
pse_pricedata['ticker'] = pse_pricedata['ticker'].map(ticker_map)
pse_pricedata = pse_pricedata.rename(columns={'date': 'date_id',
                                              'ticker': 'ticker_id'})
pse_pricedata['exchange_id'] = 1
# %%
# check if ticker and its corresponding date_id already exist int the database
x1 = pse_pricedata[['date_id', 'ticker_id']]
x2 = db_ohlcv[['date_id', 'ticker_id']]

db_ohlcv_inputs = pse_pricedata.loc[~x1.set_index(
    list(x1.columns)).index.isin(x2.set_index(list(x2.columns)).index)]

# %%

if db_ohlcv_inputs.empty == False:
    db_ohlcv_inputs.to_sql(name='ohlcv', con=engine,
                           index=False, if_exists='append')
    print("trading db's HISTORICAL PRICE DATA has been updated today,", date_today)

# %%
# and db_ticker_input.empty == True and delisted_ticker.empty == True
if db_date_inputs.empty == True and db_ohlcv_inputs.empty == True:
    print("NO CHANGES HAS BEEN MADE, trading datebase is up to date,", date_today)
# %%
print("{0} mins has lapsed".format((time.time() - start_time)/60))
# %%
