""" Demo basic extraction job using a public datasource (from wikimedia). 
No authentication needed here. """
from core.etl_utils import ETL_Base, Commandliner
import requests
# from io import StringIO

class Job(ETL_Base):
    def transform(self):
        # import ipdb; ipdb.set_trace()

        url = self.jargs.api_inputs['path']
        resp = requests.get(url, allow_redirects=True)
        # response = urllib.request.urlopen(url)
        # data = response.read()

        local_path = 'tmp/tmp_file.csv.gz'
        open(local_path, 'wb').write(resp.content)
        sdf = self.sc_sql.read.csv(local_path, header=True)
        return sdf.repartition(1)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
