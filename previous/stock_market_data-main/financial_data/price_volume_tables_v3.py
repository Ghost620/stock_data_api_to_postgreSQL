import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.ssh_client import open_ssh_tunnel
import re
from itertools import pairwise
from datetime import datetime
import pandas as pd
from multiprocessing import Pool

from financial_data.symbols_exchange_v3 import get_symbols_exchanges
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value
from table_ops.create_table import create_table_v2
from table_ops.save_data import save_v3

import logging
logger = logging.getLogger(__name__)




class PriceVolumeTables():
    '''
    To pull data from fmpcloud api endpoint (historical-price-full),
    to store prices and volumes data of interested symbols.
    
    :param url: String url to fetch data from
    '''
    def __init__(self, API_KEY, end_point, symbol, period, limit, session=create_session()):
        self.API_KEY, self.end_point, self.symbol, self.period, self.limit, self.session = API_KEY, end_point, symbol, period, limit, session
    
    def fetch_data(self):
        try:
            session = create_session()
            logger.info('fetching record for endpoint %s symbol %s for period %s'%(self.end_point, self.symbol, self.period))
            url = f"https://fmpcloud.io/api/v3/{self.end_point}/{self.symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            data = get_api(session, url)
            if data:
                # Daily Price per Ticker
                temp_price  = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'close':self.symbol})[['date',self.symbol]]
                # Daily Price per Ticker
                temp_volume = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'volume':self.symbol})[['date',self.symbol]]
            return temp_price.to_dict('records'), temp_volume.to_dict('records')

        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()




@open_ssh_tunnel
def main_func():
    try:
        port = conn_params['port']
        # no of columns in each table (max: 1500)
        table_size = 500
        
        symbols, limit, table_list_count, total_count = get_symbols_exchanges(api_key, table_size, port=port)
        table_count = len(table_list_count)-1
        end_point, period = 'historical-price-full', 'annual'

        price_table_names = ['daily_price_per_ticker_%s'%str(i+1) for i in range(total_count-table_count,total_count)]
        volume_table_names = ['daily_volume_per_ticker_%s'%str(i+1) for i in range(total_count-table_count,total_count)]
        
        for count, (price_table, vol_table) in enumerate(zip(price_table_names, volume_table_names)):
            logger.info('taking limit value as %s'%limit)
            symbols_temp = symbols[table_list_count[count]:table_list_count[count+1]]

            create_table_v2(symbols_temp, price_table, limit, pk='date', port=port)
            create_table_v2(symbols_temp, vol_table, limit, pk='date', port=port)
        
            logger.info('fetching data from %s for %s companies for period %s'%(end_point, len(symbols_temp), period))
            for symbol in symbols_temp:
                pvt = PriceVolumeTables(api_key, end_point, symbol, period, limit)
                # create data from end_point
                price_vals, volume_vals = pvt.fetch_data()
                if price_vals:
                    # save data to table
                    save_v3(price_vals, price_table, port=port)
                    save_v3(volume_vals, vol_table, port=port)
    except Exception as e:
            logger.error(e)




if __name__ == '__main__':
    main_func()