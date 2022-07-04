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

        contributors = contributors[contributors['contributions'] > 20]
        self.logger.info(f"Size of contributors table after filtering {len(contributors)}")
        keep_contribs = ['avatar_url', 'html_url', 'organizations_url', 'type', 'site_admin', 'contributions', 'repo_name', 'repo_full_name']  # other: 'login', 'id', 'node_id'

        data = []
        for ii, row in contributors.iterrows():
            self.logger.info(f"About to pull committer info from {row['login']} for repo {row['repo_name']}")
            url = f"https://api.github.com/repos/{row['login']}/{row['repo_name']}/commits?per_page=1"  # TODO: check stats/contributors instead of contributors
            resp, data_line = pull_1page(url, headers)
            if getattr(resp, 'status_code', None) != 200 or not (isinstance(data_line, list) and len(data_line) > 0):
                continue

            if 'last' in resp.links:
                commits = resp.links['last']['url'].split('per_page=1&page=')[-1]
            else:
                commits = None

            pm = {}
            try:
                pm['email'] = data_line[0]['commit']['author']['email']
                pm['name'] = data_line[0]['commit']['author']['name']
                pm['last_commit'] = data_line[0]['commit']['author']['date']
                pm['login'] = data_line[0].get('author', {}).get('login', None) if data_line[0].get('author') else None
            except Exception as ex:
                pm['email'], pm['name'], pm['last_commit'], pm['login'] = None, None, None, None
                self.logger.info(f"Failed getting nested fields, data: {data_line}, error: {ex}")

            orig = row.to_dict()
            orig = {key: value for key, value in orig.items() if key in keep_contribs}
            data_line = [{**item, **pm, **orig} for item in data_line]
            data.extend(data_line)
            self.logger.info("Finished pulling committer info")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields :{df.columns}")

        keep = ['sha', 'node_id'] + keep_contribs + list(pm.keys())
        return df[keep]


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
