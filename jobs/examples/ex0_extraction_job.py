""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import os
import pandas as pd
# from io import StringIO
import io
from zipfile import ZipFile

class Job(ETL_Base):
    def transform(self):
        url = self.jargs.api_inputs['path']
        resp = requests.get(url, allow_redirects=True)
        self.logger.info('Finished reading file from {}.'.format(url))

        fp = io.BytesIO(resp.content)
        pdf = pd.read_csv(fp, compression='gzip')
        # files = ZipFile(inmem_file)
        # files.open("2020_05_16/Summary_stats_all_locs.csv")
        # pdf = pd.read_csv(inmem_file)
        # inmem_file = io.BytesIO(resp.read())
        # open(inmem_file, 'wb').write(resp.content)
        # import ipdb; ipdb.set_trace()

        # Save as dataframe
        # pdf = pd.read_csv(StringIO(jobresults))
        # pdf = pd.read_csv(local_path)
        # sdf = self.sc_sql.createDataFrame(pdf)
        # return sdf.repartition(1)
        return pdf


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
