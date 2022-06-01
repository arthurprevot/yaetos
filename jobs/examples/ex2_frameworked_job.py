from yaetos.etl_utils import ETL_Base, Commandliner
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col


class Job(ETL_Base):
    def transform(self, some_events, other_events):
        """For demo only. Functional but no specific business logic."""

        df = self.query("""
            SELECT se.timestamp, se.session_id, se.group, se.action
            FROM some_events se
            JOIN other_events oe on se.session_id=oe.session_id
            WHERE se.action='searchResultPage' and se.n_results>0
            """)

        udf_format_datetime = udf(self.format_datetime, StringType())

        events_cleaned = df \
            .withColumn('timestamp_obj', udf_format_datetime(df.timestamp).cast("timestamp")) \
            .where(col('timestamp').like("%2.016%") is False)

        events_cleaned.createOrReplaceTempView("events_cleaned")

        self.sc_sql.registerFunction("date_diff_sec", self.date_diff_sec, IntegerType())
        output = self.query("""
            WITH
            session_times as (
                SELECT timestamp, timestamp_obj, session_id, group, action,
                  first_value(timestamp_obj) over (partition by session_id order by timestamp_obj) as first_timestamp,
                  first_value(timestamp_obj) over (partition by session_id order by timestamp_obj desc) as last_timestamp  -- last_value(timestamp_obj) didn't work
                FROM events_cleaned
            ),
            session_grouped as (
                select session_id, first_timestamp, last_timestamp,
                  date_diff_sec(first_timestamp, last_timestamp) as delta_sec,
                  count(case when action='searchResultPage' then 1 else NULL end) as search_count,
                  count(case when action='visitPage' then 1 else NULL end) as visit_count,
                  count(case when action='searchResultPage' or action='visitPage' then 1 else NULL end) as search_or_visit_count
                from session_times
                group by session_id, first_timestamp, last_timestamp
            )
            select *
            from session_grouped
            order by delta_sec desc, first_timestamp
            """)
        return output

    @staticmethod
    def format_datetime(wiki_dt):
        dt = {}
        dt['year'] = wiki_dt[:4]
        dt['month'] = wiki_dt[4:6]
        dt['day'] = wiki_dt[6:8]
        dt['hour'] = wiki_dt[8:10]
        dt['minute'] = wiki_dt[10:12]
        dt['sec'] = wiki_dt[12:14]
        return '{year}-{month}-{day} {hour}:{minute}:{sec}'.format(**dt)

    @staticmethod
    def date_diff_sec(x, y):
        return int((y - x).total_seconds())


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
