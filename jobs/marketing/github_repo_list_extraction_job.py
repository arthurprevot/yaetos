from yaetos.etl_utils import ETL_Base, Commandliner
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_all_pages


class Job(ETL_Base):
    def transform(self):
        # API: https://docs.github.com/en/rest/repos/repos#list-public-repositories
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(self.jargs.api_inputs['creds'], 'token')
        headers = {'Authorization': "Token " + token}

        url = f"https://api.github.com/repositories"
        repos = pull_all_pages(url, headers)
        return pd.DataFrame(repos)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml',
            'api_inputs': {'creds': 'github'}}
    Commandliner(Job, **args)
