from yaetos.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher, pdf_to_sdf
from yaetos.db_utils import pdf_to_sdf
from libs.python_db_connectors.query_oracle import query as query_oracle
from sqlalchemy import types


class Job(ETL_Base):

    OUTPUT_TYPES = {
        'session_id': types.VARCHAR(16),
        'count_events': types.INT(),
        }

    def transform(self):
        cred_profiles = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage)
        query_str = """
            SELECT session_id, count_events
            FROM test_ex5_pyspark_job
            where rownum < 200
            """
        df = query_oracle(query_str, db=self.db_creds, creds_or_file=cred_profiles)
        # TODO: Check to get OUTPUT_TYPES from query_oracle, so not required here.
        sdf = pdf_to_sdf(df, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
