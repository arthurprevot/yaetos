from core.etl_utils import etl_base


class Job(etl_base):
    def transform(self, processed_events):
        # import ipdb; ipdb.set_trace()
        df = self.query("""
            SELECT timestamp_obj as other_timestamp, *
            FROM processed_events se
            order by timestamp_obj
            """)
        return df


if __name__ == "__main__":
    # Job().commandline_launch(aws_setup='perso')

    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    # app_name = self.get_app_name(args)
    app_name = 'test_app'
    sc = SparkContext(appName=app_name)
    sc_sql = SQLContext(sc)
    args = {'storage':'local'}

    from jobs.examples.ex3_dependant_job import Job as Ex3_Dependant_Job
    ex3_dependant_job = Ex3_Dependant_Job()
    ex3_incremental_job = Job()
    # app_name = 'ex3_dependant_job'
    dep = ex3_dependant_job.etl(sc, sc_sql, args)
    # app_name = 'ex3_incremental_job'
    loaded_inputs = {'processed_events':dep}
    out = ex3_incremental_job.etl(sc, sc_sql, args, loaded_inputs)

    # Notes
    # - outputs of inc jobs may have to be updated to have full table as output.
    # - need to have way to inject outputs into inputs of next job.
    # - need to handle sql jobs.
    # - could use networkx to build dependency of jobs.
    # -
