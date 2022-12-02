import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.ssh_client import open_ssh_tunnel
from table_ops.table_ops import set_value
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine

import logging
logger = logging.getLogger(__name__)



# save only new values based on pk
def save(values, table_name, symbol=None, pk='tsid', port=5432):
    try:
        # filter for new values
        df = pd.DataFrame(values)
        conn_params['port'] = port
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        if symbol==None: cur.execute('SELECT %s from %s'%(pk,table_name))
        else: cur.execute("SELECT %s from %s where symbol = '%s'"%(pk,table_name,symbol))
        
        model_pk_set = set([val[0] for val in cur.fetchall()])
        conn.close()
        sheet_pk_set = set(df[pk])
        pk_to_save = sheet_pk_set - model_pk_set

        # get new records from df
        l = [df[df.loc[:,pk] == val] for val in pk_to_save]
        if l:
            df = pd.concat(l, ignore_index=True)
            df['created_at'] = datetime.now()

            # save new values only to table
            engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{database}".format(**conn_params))
            conn = engine.connect()
            df.to_sql(table_name, conn, if_exists='append', index=False)
            conn.close()
            logger.info("saved %s new values for %s"%(len(df), table_name))
        else:
            logger.info("no new values to save for %s"%table_name)
    
    except Exception as e:
        logger.error(e)



# replace existing database values with new values
def save_v2(values, table_name, port=5432):
    try:
        df = pd.DataFrame(values)
        df['created_at'] = datetime.now()
        
        conn_params['port'] = port
        engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{database}".format(**conn_params))
        conn = engine.connect()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info("saved %s new values for %s"%(len(df), table_name))

    except Exception as e:
        logger.error(e)



# save one column at a time 
def save_v3(values, table_name, port=5432):
    try:
        conn_params['port'] = port
        engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{database}".format(**conn_params))
        conn = engine.connect()
        logger.info("saving %s new values for %s"%(len(values), table_name))
        s = ''
        for v in values:
            k, = (set(v.keys()) - {'date'})
            s += '''
                update %s 
                set "%s" = '%s' 
                where date = '%s'; 
                '''%(table_name, k, v[k], v['date'])
        set_value(s, port)
        conn.close()

    except Exception as e:
        logger.error(e)