import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.table_ops import open_ssh_tunnel
import re
import json
import hashlib
from multiprocessing import Pool
import pandas as pd

from financial_data.symbols_exchange_v2 import get_symbols_exchanges
from helper_funcs.get_api import get_api, create_session
from table_ops.create_table import create_table
from table_ops.save_data import save

import logging
logger = logging.getLogger(__name__)




class GeneralScriptOne():

    def __init__(self, API_KEY, session, end_point, url_version, symbol, exchange, period, limit):
        self.API_KEY, self.session, self.end_point, self.url_version, self.symbol, self.exchange, self.period, self.limit = API_KEY, session, end_point, url_version, symbol, exchange, period, limit
    
    def fetch_data(self):
        try:
            logger.info('fetching data from %s for %s symbol for period %s'%(self.end_point, self.symbol, self.period))
            if self.url_version == 'v3': url = f"https://fmpcloud.io/api/v3/{self.end_point}/{self.symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            elif self.url_version == 'v4': url = f"https://fmpcloud.io/api/v4/{self.end_point}?symbol={self.symbol}&period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            data = get_api(self.session, url)
            if data:
                df = pd.DataFrame(data)
                # add 'symbol' if not exists
                if 'symbol' not in df.columns:
                    df.insert(loc=0, column='symbol', value=self.symbol)
                # add 'exchangeShortName' if not exists
                if 'exchangeShortName' not in df.columns:
                    df.insert(2, 'exchangeShortName', self.exchange)
                
                # incase the table does not have the date column, 
                # add the endpoint to the set exception_tables_set
                exception_tables_set = {'key-metrics-ttm','ratios-ttm','profile','stock_peers'}
                
                # add 'calendarYear' if not exists
                if 'calendarYear' not in df.columns and self.end_point not in exception_tables_set:
                    df.insert(3, 'calendarYear', df['date'].map(lambda x:x.split('-')[0]))
                
                # convert column names to snake_case
                df.rename(columns={col:self.camel_to_snake(col) for col in df.columns}, inplace=True)
                
                # add 'hash' primary key
                df['hash'] = df.apply(lambda x:self.dict_hash(dict(x)),axis=1)
                
                if self.period == 'quarter': count = int(len(df)/4)
                else: count = len(df)
                logger.info('Receieved %s years of data for company %s'%(count, self.symbol))
                return df.to_dict('records')
        
        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
    def dict_hash(self, data) -> str:
        """SHA256 hash of a dictionary."""
        dhash = hashlib.sha256()
        encoded = json.dumps(data, sort_keys=True).encode()
        dhash.update(encoded)
        return dhash.hexdigest()




def main(*args):
    """
    *args -> (('income-statement', 'annual', 30, 'annual_income_statement','v3'),)
    end_point, period, limit, table_name, url_version = 'income-statement', 'annual', 30, 'annual_income_statement','v3'
    """
    try:
        end_point, period, limit, table_name, url_version, port = args[0]
        logger.info('port: %s'%port)
        # get symbols and exchange data
        symbols, exchanges = get_symbols_exchanges(api_key, table_name, port=port)
        
        session = create_session()
        logger.info('fetching data from %s for %s companies for period %s'%(end_point, len(symbols), period))
        for symbol, exchange in zip(symbols, exchanges):
            gs = GeneralScriptOne(api_key, session, end_point, url_version, symbol, exchange, period, limit)
            # create data from end_point
            values = gs.fetch_data()
            if values:
                # create table if not exists for end_point
                create_table(values, table_name, pk='hash', port=port)
                # save data to table
                save(values, table_name, symbol, pk='hash', port=port)
    except Exception as e:
            logger.error(e)

@open_ssh_tunnel
def mp_main(args):
    args = [(*arg, conn_params['port']) for arg in args]
    # multiprocessing
    pool = os.cpu_count()
    with Pool(pool) as p:
        logger.info("starting pool of %s workers"%pool)
        p.map(main, args)




if __name__ == '__main__':

    # fundamental tables
    args = [
        # (endpoint, period, limit, table_name, url_version)
        ('profile','annual', 30, 'company_profile','v3'),
        ('shares_float','annual', 30, 'shares_float','v4'),
        ('stock_peers','annual', 30, 'stock_peers','v4'),
    ]
    mp_main(args)