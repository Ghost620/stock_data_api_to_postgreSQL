import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry

import logging
logger = logging.getLogger(__name__)

def create_session():
    """
    create a requests Sesssion that defines paramerters 
    like retry count, backoff time ({backoff factor} * (2 ** ({number of total retries} - 1)))
    """
    try:
        session = requests.Session()
        retry = Retry(total=5, connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        return session
    except Exception as e:
        logger.error(e)

def get_api(session, url):
    """
    get call based on session created
    return data in json format, if status code is 200
    """
    try:
        r = session.get(url, headers={'Content-Type': 'application/json'})
        if r.status_code == requests.codes.ok: 
            return r.json()
    except Exception as e:
        logger.error(e)