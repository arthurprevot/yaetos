from yaetos.etl_utils import ETLBase, Commandliner
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col


class Job(ETLBase):
    def transform(self, some_events):
        df = self.query(
            """
            SELECT se.session_id, session_length, session_length*2 as doubled_length
            FROM some_events se
            """
        )
        return df


if __name__ == "__main__":
    args = {"job_param_file": "conf/jobs_metadata.yml"}
    Commandliner(Job, **args)
