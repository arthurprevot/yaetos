from yaetos.etl_utils import ETL_Base, Commandliner
import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_all_pages


class Job(ETL_Base):
    def transform(self):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        self.logger.info(f"About to pull data")

        url = f"https://api.github.com/repos/arthurprevot/yaetos/traffic/clones"
        clones = pull_all_pages(url, headers)
        clones_ts = clones[0]['clones']
        clones_ts = [{**item, 'category': 'clones'} for item in clones_ts]
        data.extend(clones_ts)

        url = f"https://api.github.com/repos/arthurprevot/yaetos/traffic/views"
        views = pull_all_pages(url, headers)
        views_ts = views[0]['views']
        views_ts = [{**item, 'category': 'views'} for item in views_ts]
        data.extend(views_ts)

        self.logger.info(f"Finished pulling data")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
