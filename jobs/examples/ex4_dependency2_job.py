from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, some_events):
        # import ipdb; ipdb.set_trace()
        some_events['doubled_length'] = some_events['session_length'].apply(lambda cell: cell*2)

        # df = self.query("""
        #     SELECT se.session_id, session_length, session_length*2 as doubled_length
        #     FROM some_events se
        #     """)
        return some_events


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
