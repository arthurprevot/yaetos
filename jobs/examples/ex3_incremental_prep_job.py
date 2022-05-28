from yaetos.etl_utils import ETL_Base, Commandliner
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col


class Job(ETL_Base):
    def transform(self, some_events):

        udf_format_datetime = udf(self.format_datetime, StringType())

        events_cleaned = some_events.withColumn(
            "timestamp_obj",
            udf_format_datetime(some_events.timestamp).cast("timestamp"),
        ).where(col("timestamp").like("%2.016%") == False)
        return events_cleaned


if __name__ == "__main__":
    args = {"job_param_file": "conf/jobs_metadata.yml"}
    Commandliner(Job, **args)
