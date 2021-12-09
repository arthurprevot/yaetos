from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, processed_events):
        df = self.query("""
            SELECT timestamp_obj as other_timestamp, *
            FROM processed_events se
            order by timestamp_obj
            """)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
