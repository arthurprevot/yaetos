""" Demo basic extraction job using a public datasource (from wikimedia) """
from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from io import BytesIO
from yaetos.env_dispatchers import FS_Ops_Dispatcher, Cred_Ops_Dispatcher


class Job(ETL_Base):
    def transform(self):

        owner='awslabs'
        creds_section=self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        # import ipdb; ipdb.set_trace()

        headers = {'Authorization':"Token "+token}

        repos=[]
        for page_num in range(1,5):  # 300
            url=f"https://api.github.com/users/{owner}/repos?page={page_num}"
            try:
            # to find all the repos' names from each page
                repo=requests.get(url,headers=headers).json()
                # import ipdb; ipdb.set_trace()
                repos.extend(repo)
                self.logger.info(f"pulling from page {page_num}")
            except:
                self.logger.info(f"Couldn't pull data from {url}")

        # self.logger.info(f"fields available: {repos[0][0].keys()}")
        # import ipdb; ipdb.set_trace()
        return pd.DataFrame(repos)
        # url = self.jargs.api_inputs['path']
        # resp = requests.get(url, allow_redirects=True)
        # self.logger.info('Finished pulling file from {}.'.format(url))
        # fp = BytesIO(resp.content)
        # return pd.read_csv(fp, compression='gzip')


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml',
            'api_inputs': {'creds': 'github'}
    }
    Commandliner(Job, **args)
