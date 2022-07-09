from yaetos.etl_utils import ETL_Base, Commandliner
# import requests
import pandas as pd
from yaetos.env_dispatchers import Cred_Ops_Dispatcher
from jobs.marketing.github_utils import pull_all_pages


class Job(ETL_Base):
    def transform(self, github_accounts_man):
        creds_section = self.jargs.api_inputs['creds']
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, aws_creds='yaetos/connections', local_creds=self.jargs.connection_file)
        token = creds.get(creds_section, 'token')
        headers = {'Authorization': "Token " + token}

        data = []
        # for owner in github_accounts_man['account_name'].tolist():
        for ii, row in github_accounts_man.iterrows():
            self.logger.info(f"About to pull from owner {row['account_name']}")

            url = f"https://api.github.com/users/{row['account_name']}"
            # accounts_info = self.get_accounts_info(owner, headers)
            accounts_info = pull_all_pages(url, headers)

            accounts_info = {**row, **accounts_info}
            data.append(accounts_info)
            self.logger.info(f"Finished pulling all repos in {row['account_name']}")
        df = pd.DataFrame(data)
        self.logger.info(f"Fields {df.columns}")
        return df

    # @staticmethod
    # def get_accounts_info(owner, headers):
    #     url = f"https://api.github.com/users/{owner}"
    #     request = requests.get(url, headers=headers)
    #     if request.status_code == 200:
    #         return request.json()
    #     else:
    #         return None


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
