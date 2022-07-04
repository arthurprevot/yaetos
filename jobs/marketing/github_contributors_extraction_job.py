from yaetos.etl_utils import ETL_Base, Commandliner
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_all_pages
import pandas as pd
# TODO:
# - add org associated to contributor (first org) and number of orgs if >1, same for subscriptions, saame for number of followers
# - move to graphQL


class Job(ETL_Base):
    def transform(self, repos):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        for ii, row in repos.iterrows():
            self.logger.info(f"About to pull contributors from repo {row['full_name']}")
            url = f"https://api.github.com/repos/{row['owner']}/{row['name']}/contributors?per_page=100"  # TODO: check stats/contributors instead of contributors
            repo_contribs = pull_all_pages(url, headers)
            repo_contribs = [{**item, 'repo_full_name': row['full_name'], 'repo_name': row['name']} for item in repo_contribs]
            data.extend(repo_contribs)
            self.logger.info(f"Finished pulling all contributors in {row['full_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        keep = ['login', 'id', 'node_id', 'avatar_url', 'html_url', 'organizations_url', 'type', 'site_admin', 'contributions', 'repo_name']
        return df[keep]


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
