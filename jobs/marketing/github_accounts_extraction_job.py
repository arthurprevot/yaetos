from yaetos.etl_utils import ETL_Base, Commandliner
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_1page


class Job(ETL_Base):
    def transform(self, github_accounts_man):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(self.jargs.api_inputs['creds'], 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        for ii, row in github_accounts_man.iterrows():
            self.logger.info(f"About to pull from owner {row['account_name']}")
            url = f"https://api.github.com/users/{row['account_name']}"
            resp, data_line = pull_1page(url, headers)
            if resp:
                data_line = {**row, **data_line}
                data.append(data_line)
            self.logger.info(f"Finished pulling all repos in {row['account_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
