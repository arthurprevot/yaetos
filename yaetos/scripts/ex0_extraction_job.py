""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import os
import pandas as pd


class Job(ETL_Base):
    def transform(self):
        url = self.jargs.api_inputs['path']
        resp = requests.get(url, allow_redirects=True)
        self.logger.info('Finished reading file from {}.'.format(url))

        # Save to local
        tmp_dir = 'tmp'
        os.makedirs(tmp_dir, exist_ok = True)
        local_path = tmp_dir+'/tmp_file.csv.gz'
        open(local_path, 'wb').write(resp.content)  # creating local copy, necessary for sc_sql.read.csv, TODO: check to remove local copy step.
        self.logger.info('Copied file locally at {}.'.format(local_path))

        # Save as dataframe
        pdf = pd.read_csv(local_path)
        sdf = self.sc_sql.createDataFrame(pdf)
        return sdf.repartition(1)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
