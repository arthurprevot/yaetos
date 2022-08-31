from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        df = self.query("""
            SELECT
                session_id, session_length, doubled_length, quadrupled_length,
                session_length*8 as octupled_length
            FROM some_events se
            """)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
