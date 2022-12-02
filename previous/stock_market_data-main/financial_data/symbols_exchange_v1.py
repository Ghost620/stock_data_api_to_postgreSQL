import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
import pandas as pd
from helper_funcs.get_api import get_api, create_session

import logging
logger = logging.getLogger(__name__)

# get company names
def get_symbols_exchanges(API_KEY, table_name, port=5432):
    '''
    To pull the symbols and exchange data via the fmpcloud api endpoint (stock).
    
    :param url: String url to fetch data from
    '''
    try:
        logger.info('fetching symbols and exchanges data')
        url = "https://fmpcloud.io/api/v3/stock/list?apikey=%s"%API_KEY
        session = create_session()
        data = get_api(session, url)
        if data:
            df = pd.DataFrame(data)
            # get only symbols from 'NASDAQ','NYSE' exchanges
            df = df.set_index('exchangeShortName').loc[['NASDAQ','NYSE'],'symbol'].reset_index()
            logger.info('fetched symbols and exchanges for %s companies'%len(df))
            return df['symbol'].values, df['exchangeShortName'].values
    
    except Exception as e:
        logger.error(e)