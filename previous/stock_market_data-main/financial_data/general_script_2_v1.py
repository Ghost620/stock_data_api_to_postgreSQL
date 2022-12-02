import sys
sys.path.insert(0,'C:/Users/bayoa/PythonProjects/stock_market_data-main')

from settings.settings import *
from table_ops.table_ops import open_ssh_tunnel
import re
import pandas as pd

from table_ops.create_table import create_table
from table_ops.save_data import save_v2
from helper_funcs.get_api import get_api, create_session

import logging
logger = logging.getLogger(__name__)




class GeneralScriptTwo():

    def __init__(self, API_KEY, end_point, url_version):
        self.API_KEY, self.end_point, self.url_version = API_KEY, end_point, url_version
    
    def fetch_data(self):
        try:
            session = create_session()
            url = f"https://fmpcloud.io/api/{self.url_version}/{self.end_point}?apikey={self.API_KEY}"
            data = get_api(session, url)
            if data:
                df = pd.DataFrame(data)
                # convert column names to snake_case
                df.rename(columns={col:self.camel_to_snake(col) for col in df.columns}, inplace=True)
                return df.to_dict('records')
        
        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()




@open_ssh_tunnel
def main(args):
    port = conn_params['port']
    for arg in args:
        try:
            end_point, table_name, pk, url_version = arg

            logger.info('fetching data from endpoint %s'%(end_point))
            gs = GeneralScriptTwo(api_key, end_point, url_version)
            # create data from end_point
            values = gs.fetch_data()
            if values:
                # create table if not exists for end_point
                create_table(values, table_name, pk=pk, port=port)
                # save data to table
                save_v2(values, table_name, port=port)
            
        except Exception as e:
            logger.error(e)




if __name__ == '__main__':
    args = [
        # (end_point, table_name, primary_key, url_version)
        ('gainers', 'stock_market_performances', 'ticker', 'v3'),
        ('earning_calendar', 'earning_calendar', 'symbol', 'v3'),
    ]
    main(args)