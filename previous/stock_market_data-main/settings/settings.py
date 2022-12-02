import sys
import os
from dotenv import load_dotenv
load_dotenv()

# sys.path.append(os.path.dirname(os.path.realpath(__file__)))

api_key = os.environ.get('API_KEY')

LOGGING = {
	'version': 1,
	'disable_existing_loggers': False,
	'formatters': {
		'simple': {
			'format': '[%(asctime)s] %(levelname)s %(message)s',
		},
		'verbose': {
			'format': '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
		},
	},
	'handlers': {
		'console': {
			'class': 'logging.StreamHandler',
			'level': 'INFO',
			'formatter': 'verbose'
		},
	},
	'loggers': {
		'': {
			'handlers': ['console'],
			'level': 'INFO',
			'propagate': True,
		},
	},
}
import logging
import logging.config

logging.config.dictConfig(LOGGING)

# Database configs
DB_ENV_PROD 	= int(os.environ.get('DB_ENV_PROD'))

if DB_ENV_PROD == 1:
	conn_params = {
		'database'	: os.environ.get('CLOUD_DB_NAME'), 
		'user'		: os.environ.get('CLOUD_DB_USER'), 
		'password'	: os.environ.get('CLOUD_DB_PASSWORD'), 
		'host'		: os.environ.get('DB_HOST'), 
		'port'		: int(os.environ.get('DB_PORT'))
		}
else:
	conn_params = {
		'database'	: os.environ.get('LOCAL_DB_NAME'), 
		'user'		: os.environ.get('LOCAL_DB_USER'), 
		'password'	: os.environ.get('LOCAL_DB_PASSWORD'), 
		'host'		: os.environ.get('DB_HOST'), 
		'port'		: int(os.environ.get('DB_PORT'))
		}

REMOTE_HOST 	= os.environ.get('REMOTE_HOST')
REMOTE_USERNAME = os.environ.get('REMOTE_USERNAME')
PKEY_PATH		= 'C:/Users/bayoa/PythonProjects/stock_market_data-main/stock-mkt-key.pem'
