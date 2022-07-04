from yaetos.etl_utils import ETL_Base, Commandliner
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_1page
import pandas as pd


class Job(ETL_Base):
    def transform(self, contributors):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        # contributors = contributors[contributors['contributions'] > 20]
        data = []
        # for row in repos.iterrows():
        for row in list(contributors.iterrows())[:10]:
            self.logger.info(f"About to pull committer info from {row[1]['login']} for repo {row[1]['repo_name'].split('/')[-1]}")

            url = f"https://api.github.com/repos/{row[1]['login']}/{row[1]['repo_name'].split('/')[-1]}/commits?per_page=1"  # TODO: check stats/contributors instead of contributors
            resp, data_line = pull_1page(url, headers)
            # import ipdb; ipdb.set_trace()
            if getattr(resp, 'status_code', None) != 200 or not isinstance(data_line, list):
                continue

            if 'last' in resp.links:
                commits = resp.links['last']['url'].split('per_page=1&page=')[-1]
            else:
                commits = None

            pm = {}
            if len(data_line) > 0:
                # import ipdb; ipdb.set_trace()
                pm['email'] = data_line[0]['commit']['author']['email']
                pm['name'] = data_line[0]['commit']['author']['name']
                pm['last_commit'] = data_line[0]['commit']['author']['date']
                pm['login'] = data_line[0]['author']['login']
            else:
                pm['email'], pm['name'], pm['last_commit'], pm['login'] = None, None, None, None

            # import ipdb; ipdb.set_trace()
            data_line = [{**item, **pm} for item in data_line]
            data.extend(data_line)
            self.logger.info("Finished pulling committer info")
        df = pd.DataFrame(data)
        # import ipdb; ipdb.set_trace()
        self.logger.info(f"Fields {df.columns}")
        keep = ['sha', 'node_id'] + list(pm.keys())
        return df[keep]


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
