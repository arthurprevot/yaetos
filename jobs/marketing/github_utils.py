import requests
from yaetos.logger import setup_logging
logger = setup_logging('Job')


def pull_all_pages(url, headers):
    pages_data = []
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
