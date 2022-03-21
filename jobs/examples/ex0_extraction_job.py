""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import os

class Job(ETL_Base):
    def transform(self):
        url = self.jargs.api_inputs['path']
        resp = requests.get(url, allow_redirects=True)
        self.logger.info('Finished reading file from {}.'.format(url))
        tmp_dir = 'file://{}/tmp'.format(os.getcwd())
        os.makedirs(tmp_dir, exist_ok = True)
        local_path = tmp_dir+'/tmp_file.csv.gz'
        open(local_path, 'wb').write(resp.content)  # creating local copy, necessary for sc_sql.read.csv, TODO: check to remove local copy step.
        self.logger.info('Copied file locally at {}.'.format(local_path))
        sdf = self.sc_sql.read.csv(local_path, header=True)
        return sdf.repartition(1)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
