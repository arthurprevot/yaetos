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

        data = []
        for ii, row in github_accounts.iterrows():
            self.logger.info(f"About to pull from owner {row['account_name']}")
            url = f"https://api.github.com/users/{row['account_name']}/repos"  # per_page=100
            repos_owner = pull_all_pages(url, headers)
            repos_owner = [{**subrow, 'owner': row['account_name']} for subrow in repos_owner]
            data.extend(repos_owner)
            self.logger.info(f"Finished pulling all repos in {row['account_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        keep = ['id', 'node_id', 'name', 'full_name', 'private', 'owner', 'html_url', 'description', 'created_at', 'updated_at', 'pushed_at', 'homepage', 'size', 'stargazers_count', 'watchers_count', 'forks_count', 'language', 'watchers']
        return df[keep]


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
