import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from math import ceil
import numpy as np
import pandas as pd
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value
import logging
logger = logging.getLogger(__name__)

# for price and volumes table
def get_symbols_exchanges(API_KEY, table_size, port=5432):
    '''
    To pull the symbols and exchange data via the fmpcloud api endpoint (stock).
    
    :param url: String url to fetch data from
    '''
    def bucket_table(df, table_size):
        # [0, 1500, 3000, 4500, 6000, 7500, 9000, len(df)]
        total = len(df)
        table_count = ceil(total/table_size)
        table_list_count = [i for i in range(0,total,table_size)]
        table_list_count.append(total)
        return table_list_count, table_count

    try:
        logger.info('fetching symbols and exchanges data')
        url = "https://fmpcloud.io/api/v3/stock/list?apikey=%s"%API_KEY
        session = create_session()
        data = get_api(session, url)
        if data:
            df = pd.DataFrame(data)
            # get only symbols from 'NASDAQ','NYSE' exchanges
            df = df.set_index('exchangeShortName').loc[['NASDAQ','NYSE'],'symbol'].reset_index()

            # create list based on table size
            table_list_count, table_count = bucket_table(df, table_size)
            table_list_name = ['daily_price_per_ticker_%s'%str(i+1) for i in range(table_count)]
            for count, table_name in enumerate(table_list_name):
                try:
                    limit = 1000
                    s = "SELECT "
                    for symbol in df['symbol'].values[table_list_count[count]:table_list_count[count+1]]:
                        s += 'max("%s"), '%(symbol)
                    s += 'max(date) from %s t'%table_name
                    values = get_value(s, port=port)
                    if values: 
                        values = values[0]
                        limit = 5
                        try:
                            i = values.index(None)+table_size*count
                            table_list_count = [j-i for j in table_list_count if j > i]
                            table_list_count.insert(0,0)
                            limit = 1000
                            df = df.iloc[
                                range(
                                    i,
                                    len(df)
                                    )]
                            break
                        except Exception as e:
                            logger.error(e)
                            logger.info("all values filled for table %s"%table_name)
                except Exception as e:
                    logger.error(e)
                    logger.info("table %s doesn't exist"%table_name)

            logger.info('fetched symbols and exchanges for %s companies'%len(df))
            return df['symbol'].values, limit, table_list_count, table_count
    
    except Exception as e:
        logger.error(e)