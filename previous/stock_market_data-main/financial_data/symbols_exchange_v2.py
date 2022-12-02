import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
import pandas as pd
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value
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
            if table_name:
                try:
                    # order symbols based on values from existing table;
                    # i.e create a list of symbols that don't exist in database first, 
                    # and then append the remaining symbols to that list
                    values = get_value('SELECT distinct %s from %s'%('symbol',table_name), port=port)
                    model_symbol_set = set([val[0] for val in values])
                    sheet_symbol_set = set(df['symbol'])
                    symbol_to_save = list(sheet_symbol_set - model_symbol_set)
                    # symbol_to_save.extend(model_symbol_set)
                    # get new records from df
                    df = df.set_index('symbol').loc[symbol_to_save,'exchangeShortName'].reset_index()
                except:
                    logger.info("table %s doesn't exist; fetching all data"%table_name)

            logger.info('fetched symbols and exchanges for %s companies'%len(df))
            return df['symbol'].values, df['exchangeShortName'].values
    
    except Exception as e:
        logger.error(e)