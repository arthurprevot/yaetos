""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from io import BytesIO


class Job(ETL_Base):
    def transform(self):
        url = self.jargs.api_inputs['path']
        self.logger.info('Starting pulling file from {}.'.format(url))
        resp = requests.get(url, allow_redirects=True)
        self.logger.info('Finished pulling file from {}.'.format(url))
        fp = BytesIO(resp.content)
        return pd.read_csv(fp, compression='gzip')


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
