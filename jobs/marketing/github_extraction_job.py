from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
import time


class Job(ETL_Base):
    def transform(self):

        owner='awslabs'
        creds_section=self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization':"Token "+token}

        repos=[]
        for page_num in range(1,5):  # 300
            url=f"https://api.github.com/users/{owner}/repos?page={page_num}"
            try:
                repo=requests.get(url,headers=headers).json()
                repos.extend(repo)
                self.logger.info(f"pulling from page {page_num}")
            except:
                self.logger.info(f"Couldn't pull data from {url}")
            time.sleep(1./4999.)  # i.e. 5000 requests max / sec
        return pd.DataFrame(repos)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml',
            'api_inputs': {'creds': 'github'}
    }
    Commandliner(Job, **args)
