from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self):
        return None


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
