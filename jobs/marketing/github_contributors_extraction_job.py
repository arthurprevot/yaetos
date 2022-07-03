from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
import time


class Job(ETL_Base):
    def transform(self, repos):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        for row in repos.iterrows():
        # for row in list(repos.iterrows())[:10]:
            self.logger.info(f"About to pull from repo {row[1]['full_name']}")
            repo_contribs = self.get_contributors(row[1]['owner'], row[1]['name'], headers)

            repo_contribs = [{**item, 'repo_name':row[1]['full_name']} for item in repo_contribs]
            data.extend(repo_contribs)
            self.logger.info(f"Finished pulling all contributors in {row[1]['full_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        keep = ['login', 'id', 'node_id', 'avatar_url', 'html_url', 'organizations_url', 'type', 'site_admin', 'contributions', 'repo_name']
        return df[keep]

    def get_contributors(self, owner, repo, headers):
        contribs = []
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors?per_page=100"
        resp, data = self.pull_data(url, headers)
        # data = resp.json()
        contribs = data.copy() if resp else []

        while resp and 'next' in resp.links:
            next_url = resp.links['next']['url']
            resp, data = self.pull_data(next_url, headers)
            # data = resp.json()
            if resp:
                contribs.extend(data)
            time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
        return contribs

    def pull_data(self, url, headers):
        try:
            resp = requests.get(url, headers=headers)
            data = resp.json()
            self.logger.info(f"pulling from {url}")
        except Exception:
            resp = None
            data = None
            self.logger.info(f"Couldn't pull data from {url}")
        return resp, data


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
