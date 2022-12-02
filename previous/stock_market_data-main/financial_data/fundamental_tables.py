import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.table_ops import open_ssh_tunnel
import re
import pandas as pd
from multiprocessing import Pool

from financial_data.symbols_exchange_v2 import get_symbols_exchanges
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value, set_value
from table_ops.create_table import create_table
from table_ops.save_data import save

import logging
logger = logging.getLogger(__name__)



class FundamentalTables():

    def __init__(self, API_KEY, session, end_point, symbol, exchange, currency, country, period, limit):
        self.API_KEY, self.session, self.end_point, self.symbol, self.exchange, self.currency, self.country, self.period, self.limit = API_KEY, session, end_point, symbol, exchange, currency, country, period, limit

    def fetch_data(self):
        try:
            logger.info('fetching data from %s for %s symbol for period %s'%(self.end_point, self.symbol, self.period))
            url = f"https://fmpcloud.io/api/v3/{self.end_point}/{self.symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            data = get_api(self.session, url)
            if data:
                df = pd.DataFrame(data)
                # add 'symbol' if not exists
                if 'symbol' not in df.columns:
                    df.insert(loc=0, column='symbol', value=self.symbol)
                # add 'exchangeShortName' if not exists
                if 'exchangeShortName' not in df.columns:
                    df.insert(2, 'exchangeShortName', self.exchange)
                # add 'currency' if not exists
                if 'currency' not in df.columns:
                    df.insert(2, 'currency', self.currency)
                # add 'country' if not exists
                if 'country' not in df.columns:
                    df.insert(2, 'country', self.country)
                # add 'calendarYear' if not exists
                if 'calendarYear' not in df.columns and self.end_point not in {'key-metrics-ttm','ratios-ttm','profile'}:
                    df.insert(3, 'calendarYear', df['date'].map(lambda x:x.split('-')[0]))
                # add 'TSID'
                if self.end_point in {'key-metrics-ttm','ratios-ttm','profile'}:
                    df.insert(0, 'TSID', df[['symbol','exchangeShortName']].apply(lambda x:'.'.join(x), axis=1))
                    count = len(df)
                elif self.period == 'quarter':
                    df.insert(1, 'TSID', df[['symbol','exchangeShortName','calendarYear','period']].apply(lambda x:'.'.join(x), axis=1))
                    count = int(len(df)/4)
                else:
                    df.insert(1, 'TSID', df[['symbol','exchangeShortName','calendarYear']].apply(lambda x:'.'.join(x), axis=1))
                    count = len(df)
                # convert column names to snake_case
                df.rename(columns={col:self.camel_to_snake(col) for col in df.columns}, inplace=True)
                
                logger.info('Receieved %s years of data for company %s'%(count, self.symbol))
                return df.to_dict('records')
        
        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()




def main(*args):
    """
    *args -> (('income-statement', 'annual', 30, 'annual_income_statement'),)
    end_point, period, limit, table_name = 'income-statement', 'annual', 30, 'annual_income_statement'
    """
    try:
        end_point, period, limit, table_name, port = args[0]
        logger.info('port: %s'%port)
        # get symbols and exchange data
        symbols, exchanges = get_symbols_exchanges(api_key, table_name, port=port)
        
        # alter table to add new columns
        set_value("""
            alter table if exists %s 
            add column if not exists country varchar(30), 
            add column if not exists currency varchar(10)
        """%table_name, port=port)
        session = create_session()
        logger.info('fetching data from %s for %s companies for period %s'%(end_point, len(symbols), period))
        for symbol, exchange in zip(symbols, exchanges):
            # add currency column
            currency = get_value(sql=" select currency from company_profile where symbol = '%s' "%symbol, port=port)
            if currency: currency = currency[0][0]
            else: currency = None
            # add currency column
            country = get_value(sql=" select country from company_profile where symbol = '%s' "%symbol, port=port)
            if country: country = country[0][0]
            else: country = None
            # update table to backfill values
            set_value("""
                update %s 
                set country = '%s', currency = '%s' 
                where symbol = '%s'
            """%(table_name, country, currency, symbol), port=port)
            # run FundamentalTables main script
            ft = FundamentalTables(api_key, session, end_point, symbol, exchange, currency, country, period, limit)
            # create data from end_point
            values = ft.fetch_data()
            if values:
                # create table if not exists for end_point
                create_table(values, table_name, port=port)
                # save data to table
                save(values, table_name, symbol, port=port)
                
        
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
        # (endpoint, period, limit, table_name)
        ('ratios-ttm', 'annual', 30, 'annual_ratios_ttm'),
        ('key-metrics-ttm', 'annual', 30, 'annual_key_metrics_ttm'),
        ('income-statement', 'annual', 30, 'annual_income_statement'),
        ('income-statement', 'quarter', 120, 'quarter_income_statement'),
        ('income-statement-growth', 'annual', 30, 'annual_income_statement_growth'),
        ('income-statement-growth', 'quarter', 120, 'quarter_income_statement_growth'),
        ('balance-sheet-statement', 'annual', 30, 'annual_balance_sheet_statement'),
        ('balance-sheet-statement', 'quarter', 120, 'quarter_balance_sheet_statement'),
        ('balance-sheet-statement-growth', 'annual', 30, 'annual_balance_sheet_statement_growth'),
        ('balance-sheet-statement-growth', 'quarter', 120, 'quarter_balance_sheet_statement_growth'),
        ('cash-flow-statement', 'annual', 30, 'annual_cash_flow_statement'),
        ('cash-flow-statement', 'quarter', 120, 'quarter_cash_flow_statement'),
        ('cash-flow-statement-growth', 'annual', 30, 'annual_cash_flow_statement_growth'),
        ('cash-flow-statement-growth', 'quarter', 120, 'quarter_cash_flow_statement_growth'),
        ('ratios', 'annual', 30, 'annual_ratios'),
        ('ratios', 'quarter', 120, 'quarter_ratios'),
        ('key-metrics', 'annual', 30, 'annual_key_metrics'),
        ('key-metrics', 'quarter', 120, 'quarter_key_metrics')
    ]
    mp_main(args)