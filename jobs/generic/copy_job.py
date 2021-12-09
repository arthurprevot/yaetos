from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):

    def transform(self, table_to_copy):
        table_to_copy.cache()
        if table_to_copy.count() < 500000:
            table_to_copy = table_to_copy.repartition(1)
        return table_to_copy


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
