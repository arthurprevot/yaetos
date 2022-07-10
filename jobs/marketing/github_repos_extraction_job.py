from yaetos.etl_utils import ETL_Base, Commandliner
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_all_pages


class Job(ETL_Base):
    def transform(self, github_accounts):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        # github_accounts = github_accounts[:3]
        data = []
        for ii, row in github_accounts.iterrows():
            self.logger.info(f"About to pull from owner {row['account_name']}")

            url = f"https://api.github.com/users/{row['account_name']}/repos"
            # repos_owner = self.get_repos(row['account_name'], row['public_repos'], headers)
            repos_owner = pull_all_pages(url, headers)
            import ipdb; ipdb.set_trace()

            repos_owner = [{**subrow, 'owner': row['account_name']} for subrow in repos_owner]
            data.extend(repos_owner)
            self.logger.info(f"Finished pulling all repos in {row['account_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        keep = ['id', 'node_id', 'name', 'full_name', 'private', 'owner', 'html_url', 'description', 'created_at', 'updated_at', 'pushed_at', 'homepage', 'size', 'stargazers_count', 'watchers_count', 'forks_count', 'language', 'watchers']
        return df[keep]

    # def get_repos(self, owner, repo_count, headers):
    #     import time
    #     import requests
    #     repos = []
    #     pages = repo_count // 100 + 1
    #     for page_num in range(1, pages + 1):  # +1 to make it inclusive.
    #         url = f"https://api.github.com/users/{owner}/repos?page={page_num}"
    #         try:
    #             repo = requests.get(url, headers=headers).json()
    #             repos.extend(repo)
    #             self.logger.info(f"###pulling from page {page_num}")
    #         except Exception as ex:
    #             self.logger.info(f"###Couldn't pull data from {url} with {ex}")
    #         time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
    #     return repos


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
