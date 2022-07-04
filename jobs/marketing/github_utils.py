from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
import time
from yaetos.logger import setup_logging
logger = setup_logging('Job')


#def pull_all_pages(self, owner, repo, headers):
def pull_all_pages(url, headers):
    pages_data = []
    #url = f"https://api.github.com/repos/{owner}/{repo}/contributors?per_page=100"  # TODO: check stats/contributors instead of contributors
    # url = f"https://api.github.com/repos/{owner}/{repo}/contributors?per_page=100"  # TODO: check stats/contributors instead of contributors
    resp, data = pull_1page(url, headers)
    pages_data = data.copy() if resp else []

    while resp and 'next' in resp.links:
        next_url = resp.links['next']['url']
        resp, data = pull_1page(next_url, headers)
        if resp:
            pages_data.extend(data)
        time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
    return pages_data

def pull_1page(url, headers):
    try:
        resp = requests.get(url, headers=headers)
        data = resp.json()
        logger.info(f"pulling from {url}")
    except Exception:
        resp = None
        data = None
        logger.info(f"Couldn't pull data from {url}")
    return resp, data
