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
        # for row in repos.iterrows()[:10]:
        for row in list(repos.iterrows())[:10]:
            self.logger.info(f"About to pull from owner {row[1]['owner']}")
            repo_contribs = self.get_contributors(row[1]['owner'], row[1]['name'], headers)

            repo_contribs = [{**item, 'owner':row[1]['owner']} for item in repo_contribs]
            data.extend(repo_contribs)
            self.logger.info(f"Finished pulling all repos in {row[1]['owner']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        keep = ['login', 'id', 'node_id', 'avatar_url', 'html_url', 'organizations_url', 'type', 'site_admin', 'contributions', 'owner']
        return df[keep]

    # def get_contributors(self, owner, repo, headers):
    #     contribs = []
    #     # pages = repo_count // 100 + 1
    #     for page_num in range(1, 3): #pages+1):  # +1 to make it inclusive.
    #         url = f"https://api.github.com/repos/{owner}/{repo}/contributors?page={page_num}"
    #         try:
    #             data = requests.get(url, headers=headers).json()
    #             contribs.extend(data)
    #             self.logger.info(f"pulling from page {page_num}")
    #         except Exception:
    #             self.logger.info(f"Couldn't pull data from {url}")
    #         import ipdb; ipdb.set_trace()
    #         time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
    #     return contribs

    def get_contributors(self, owner, repo, headers):
        contribs = []
        # pages = repo_count // 100 + 1
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors?per_page=100"
        resp = self.pull_data(url, headers)
        data = resp.json()
        contribs = data.copy()
        # self.logger.info(f"#### {resp.links['next']}")
        # next_url = resp.links['next']['url']

        while 'next' in resp.links:
            next_url = resp.links['next']['url']
            resp = self.pull_data(next_url, headers)
            data = resp.json()
            contribs.extend(data)
            time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec

            # self.logger.info(f"#### {resp.links['next']}")
            # next_url = resp.links['next']['url']

        # resp = self.pull_data(next_url, headers)
        # self.logger.info(f"#### {resp.links['next']}")
        # next_url = resp.links['next']['url']
        #
        # resp = self.pull_data(next_url, headers)
        # self.logger.info(f"#### {resp.links}")
        # next_url = resp.links['next']['url']
        #
        # resp = self.pull_data(next_url, headers)
        # self.logger.info(f"#### {resp.links['next']}")
        # next_url = resp.links['next']['url']

        # import ipdb; ipdb.set_trace()
        # for page_num in range(1, 3): #pages+1):  # +1 to make it inclusive.
        #
        #
        #     import ipdb; ipdb.set_trace()
        #     time.sleep(1. / 4999.)  # i.e. 5000 requests max / sec
        return contribs

    def pull_data(self, url, headers):
        try:
            resp = requests.get(url, headers=headers)
            self.logger.info(f"pulling from {url}")
        except Exception:
            resp = None
            self.logger.info(f"Couldn't pull data from {url}")
        return resp


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
