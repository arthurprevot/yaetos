from yaetos.etl_utils import ETL_Base, Commandliner
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col


class Job(ETL_Base):
    def transform(self, some_events):
        df = self.query("""
            SELECT * , session_length*8 as D
            FROM some_events se
            """)
        return df


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
