import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from functools import wraps
import psycopg2
from sshtunnel import SSHTunnelForwarder

import logging
logger = logging.getLogger(__name__)

def open_ssh_tunnel(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if DB_ENV_PROD:
            logging.info('Pushing data to production database')
            tunnel = SSHTunnelForwarder((REMOTE_HOST),
                ssh_pkey=PKEY_PATH,
                ssh_username=REMOTE_USERNAME,
                remote_bind_address=(conn_params['host'],int(conn_params['port'])),
                )
            tunnel.start()
            conn_params['port'] = tunnel.local_bind_port
        else: logging.info('Pushing data to local database')

        result = func(*args, **kwargs)
        
        if DB_ENV_PROD: tunnel.stop()
        return result
    return wrapper

@open_ssh_tunnel
def test_func():
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM earning_calendar LIMIT 1")
    print(cur.fetchall())
    conn.close()

# if __name__ == '__main__':
#     test_func()