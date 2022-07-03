from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
import time


class Job(ETL_Base):
    def transform(self, github_accounts):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        for row in github_accounts.iterrows():
            if row[1]['owner'] == 'apache':
                continue
            self.logger.info(f"About to pull from owner {row[1]['owner']}")
            repos_owner = self.get_repos(row[1]['owner'], row[1]['public_repos'], headers)
            repos_owner = [{**item, 'owner':row[1]['owner']} for item in repos_owner]
            data.extend(repos_owner)
            self.logger.info(f"Finished pulling all repos in {row[1]['owner']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        # import ipdb; ipdb.set_trace()
        # keep = ['id', 'node_id', 'name', 'full_name', 'private', 'owner', 'html_url', 'description', 'created_at', 'updated_at', 'pushed_at', 'homepage', 'size', 'stargazers_count', 'watchers_count', 'forks_count', 'language', 'watchers']
        # repo_df = repo_df[keep]
        return df

    def get_repos(self, owner, repo_count, headers):
        repos = []
        pages = repo_count // 100 + 1
        for page_num in range(1, pages+1):  # +1 to make it inclusive.
            url = f"https://api.github.com/users/{owner}/repos?page={page_num}"
            try:
                repo = requests.get(url, headers=headers).json()
                repos.extend(repo)
                self.logger.info(f"pulling from page {page_num}")
            except Exception:
                self.logger.info(f"Couldn't pull data from {url}")
            time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
        return repos


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
