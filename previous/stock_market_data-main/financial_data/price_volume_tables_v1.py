import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
import re
from datetime import datetime
import pandas as pd

from financial_data.symbols_exchange_v2 import get_symbols_exchanges
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value
from table_ops.create_table import create_table
from table_ops.save_data import save

import logging
logger = logging.getLogger(__name__)

class PriceVolumeTables():
    '''
    To pull data from fmpcloud api endpoint (historical-price-full),
    to store prices and volumes data of interested symbols.
    
    :param url: String url to fetch data from
    '''
    def __init__(self, API_KEY, end_point, symbols, period, limit):
        self.API_KEY, self.end_point, self.symbols, self.period, self.limit = API_KEY, end_point, symbols, period, limit
    
    def fetch_data(self):
        try:
            session = create_session()
            final_price = pd.DataFrame(columns=['date'])
            final_volume = pd.DataFrame(columns=['date'])
            for i, symbol in enumerate(self.symbols):
                logger.info('fetching record %s for endpoint %s symbol %s for period %s'%(i, self.end_point, symbol, self.period))
                url = f"https://fmpcloud.io/api/v3/{self.end_point}/{symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
                data = get_api(session, url)
                if data:
                    try:
                        # Daily Price per Ticker
                        temp_price  = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'close':symbol})[['date',symbol]]
                        final_price = final_price.merge(temp_price, on='date', how='outer').sort_values('date',ascending=False)

                        # Daily Price per Ticker
                        temp_volume = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'volume':symbol})[['date',symbol]]
                        final_volume= final_volume.merge(temp_volume, on='date', how='outer').sort_values('date',ascending=False)
                    except Exception as e:
                        logger.error(e)
                        
            # convert column names to snake_case
            final_price.rename(columns={col:self.camel_to_snake(col) for col in final_price.columns}, inplace=True)
            final_volume.rename(columns={col:self.camel_to_snake(col) for col in final_volume.columns}, inplace=True)
            
            logger.info('Receieved %s companies price and volume records'%final_price.shape[1])
            return final_price.to_dict('records'), final_volume.to_dict('records')

        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

if __name__ == '__main__':

    # get symbols and exchange data
    port = conn_params['port']
    symbols, exchanges = get_symbols_exchanges(api_key, None, port=port)
    try:
        # define limit based on records stored in db
        limit = get_value(sql="select max(date) from daily_price_per_ticker", port=port)
        if limit: 
            limit = (datetime.today().date() - limit[0][0]).days + 1
        else: 
            limit = 1000
        logger.info('taking limit value as %s'%limit)
        end_point, period = 'historical-price-full', 'annual'
        
        pvt = PriceVolumeTables(api_key, end_point, symbols, period, limit)
        # create data from end_point
        price_vals, volume_vals = pvt.fetch_data()
        if price_vals:
            # create table if not exists for end_point
            create_table(price_vals, "daily_price_per_ticker", pk='date', port=port)
            create_table(volume_vals, "daily_volume_per_ticker", pk='date', port=port)
            # save data to table
            save(price_vals, "daily_price_per_ticker", pk='date', port=port)
            save(volume_vals, "daily_volume_per_ticker", pk='date', port=port)
            
    except Exception as e:
        logger.error(e)