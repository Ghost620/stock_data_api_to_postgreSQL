import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.ssh_client import open_ssh_tunnel
import psycopg2

import logging
logger = logging.getLogger(__name__)


# save new values
def get_value(sql, port=5432):
    """
    pass query in parameter 'sql' to return result from table in db 
    """
    try:
        conn_params['port'] = port
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        val = cur.fetchall()
        conn.close()
        return val
    
    except Exception as e:
        logger.error(e)


# set value
def set_value(sql, port=5432):
    """
    pass query in parameter 'sql' to commit result from table in db 
    """
    try:
        conn_params['port'] = port
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    
    except Exception as e:
        logger.error(e)