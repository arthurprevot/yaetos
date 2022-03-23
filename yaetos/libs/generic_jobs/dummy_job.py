from yaetos.etl_utils import ETL_Base, Commandliner
from pyspark.sql.types import StructType

class Job(ETL_Base):
    def transform(self):
        return self.sc_sql.createDataFrame([], StructType([]))


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
