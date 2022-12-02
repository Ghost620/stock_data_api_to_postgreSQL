import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from datetime import datetime, timedelta
from table_ops.ssh_client import open_ssh_tunnel
from table_ops.table_ops import set_value
from numpy import dtype
import psycopg2
import pandas as pd
import logging
logger = logging.getLogger(__name__)

# create table
def create_table(values, table_name, pk='tsid', port=5432):
    try:
        # print('port: %s'%port)
        df = pd.DataFrame(values)
        conn_params['port'] = port
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        s = "CREATE TABLE IF NOT EXISTS %s ("%table_name
        for k,v in df.dtypes.items():
            if k == 'date':
                v = 'DATE'
            elif k in {'filling_date','accepted_date'}:
                v = 'TIMESTAMP WITH TIME ZONE'
                # v = 'TIMESTAMP WITHOUT TIME ZONE'
            elif k == 'tsid':
                v = 'VARCHAR (30)'
            elif k in {'symbol','exchange_short_name'}:
                v = 'VARCHAR (10)'
            elif k == 'calendar_year':
                v = 'INTEGER'
            elif v in {dtype('int64'), dtype('float64')}:
                # v = 'DECIMAL(12,6)'
                v = 'NUMERIC'
            elif v in {dtype('O')}:
                v = 'TEXT'
            else:
                v = 'TEXT'
            s+=f'"{k}" {v}, '
        s+='created_at TIMESTAMP WITH TIME ZONE, PRIMARY KEY(%s))'%pk
        # s+='created_at TIMESTAMP WITHOUT TIME ZONE, PRIMARY KEY(%s))'%pk
        cur.execute(s)
        conn.commit()
        conn.close()

    except Exception as e:
        logger.error(e)

# create price and volumes table
def create_table_v2(symbols, table_name, limit=1000, pk='date', port=5432):
    try:
        # print('port: %s'%port)
        conn_params['port'] = port
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        s = 'CREATE TABLE IF NOT EXISTS %s ( "date" DATE, '%table_name
        for symbol in symbols:
            s+=f'"{symbol}" NUMERIC, '
        s+='PRIMARY KEY(%s));'%pk
        cur.execute(s)
        conn.commit()
        days = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(limit)]
        s = ''
        for day in days:
            s += '''
            insert into %s ("date")
            Select '%s' Where not exists (
                select * from %s where date='%s'
                ); 
            '''%(table_name,day,table_name,day)
        set_value(s, port)
        conn.commit()
        conn.close()

    except Exception as e:
        logger.error(e)