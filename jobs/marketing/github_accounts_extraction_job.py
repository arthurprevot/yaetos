from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
import time
# rename to github_user_extraction_job

class Job(ETL_Base):
    def transform(self, github_accounts_man):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        for owner in github_accounts_man['account_name'].tolist():
            self.logger.info(f"About to pull from owner {owner}")
            accounts_info = self.get_accounts_info(owner, headers)
            # self.logger.info(f"Number of repos in {owner}")
            # repos_owner = self.get_repos(owner, repo_count, headers)
            # import ipdb; ipdb.set_trace()
            # repos_owner = [{**item, 'owner':owner} for item in repos_owner]
            accounts_info = {**accounts_info, 'owner':owner}
            data.append(accounts_info)
            self.logger.info(f"Finished pulling all repos in {owner}")
        # import ipdb; ipdb.set_trace()
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        # import ipdb; ipdb.set_trace()
        # keep = ['id', 'node_id', 'name', 'full_name', 'private', 'owner', 'html_url', 'description', 'created_at', 'updated_at', 'pushed_at', 'homepage', 'size', 'stargazers_count', 'watchers_count', 'forks_count', 'language', 'watchers']
        # repo_df = repo_df[keep]
        return df

    @staticmethod
    def get_accounts_info(owner, headers):
        url = f"https://api.github.com/users/{owner}"
        request = requests.get(url, headers=headers)
        if request.status_code == 200:
            return request.json() #.get('public_repos')
        else:
            return None

    # def get_repos(self, owner, repo_count, headers):
    #     repos = []
    #     pages = repo_count // 100 + 1
    #     # import ipdb; ipdb.set_trace()
    #     for page_num in range(1, pages+1):  # +1 to make it inclusive.
    #         url = f"https://api.github.com/users/{owner}/repos?page={page_num}"
    #         try:
    #             repo = requests.get(url, headers=headers).json()
    #             repos.extend(repo)
    #             self.logger.info(f"pulling from page {page_num}")
    #         except Exception:
    #             self.logger.info(f"Couldn't pull data from {url}")
    #         time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
    #     return repos


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml',
            'api_inputs': {'creds': 'github'}}
    Commandliner(Job, **args)
