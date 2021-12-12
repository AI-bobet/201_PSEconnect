# %% PSEconnect scrap historical price data of all listed companies in the Philippine Stock Exchange (PSE) including its constituent indices using scrapy framework.

import os
import sqlalchemy
import pandas as pd

# %% database connection
engine = sqlalchemy.create_engine('postgresql://{0}:{1}@{2}:{3}/trading'.format(os.environ.get('db_user'), os.environ.get('db_password'),
                                                                                os.environ.get('db_host'), os.environ.get('db_port')))

pse_stockprice = pd.read_sql_table("pse_stock_data", engine, index_col='date')
pse_indexprice = pd.read_sql_table("pse_index_data", engine, index_col='date')

# %%


class PSEconnect():

    def __init__(self):
        self.pse_stocks_info = pd.read_sql_table("pse_company_info", engine)
        self.pse_indeces_info = pd.read_sql_table("pse_index_info", engine)

    def stockprice(self, ticker, interval):
        aggregation = {'open': 'first',
                       'high': 'max',
                       'low': 'min',
                       'close': 'last',
                       'volume': 'sum'}
        data = pse_stockprice[pse_stockprice['ticker'] == ticker]
        return data.resample(interval).agg(aggregation).dropna()

    def indexprice(self, ticker, interval):
        aggregation = {'open': 'first',
                       'high': 'max',
                       'low': 'min',
                       'close': 'last'}
        data = pse_indexprice[pse_indexprice['ticker'] == ticker]
        return data.resample(interval).agg(aggregation).dropna()


# %%
if __name__ == '__main__':
    data = PSEconnect()
