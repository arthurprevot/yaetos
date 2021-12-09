""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests

class Job(ETL_Base):
    def transform(self):
        url = self.jargs.api_inputs['path']
        resp = requests.get(url, allow_redirects=True)
        local_path = 'tmp/tmp_file.csv.gz'
        open(local_path, 'wb').write(resp.content)  # creating local copy, necessary for sc_sql.read.csv
        sdf = self.sc_sql.read.csv(local_path, header=True)
        return sdf.repartition(1)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
