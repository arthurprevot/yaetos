from core.etl_utils import ETL_Base



if __name__ == "__main__":
    # Job().commandline_launch(aws_setup='perso')

    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    # app_name = self.get_app_name(args)
    app_name = 'test_app'
    sc = SparkContext(appName=app_name)
    sc_sql = SQLContext(sc)
    args = {'storage':'local'}

    # from jobs.examples.ex3_dependant_job import Job as Ex3_Dependant_Job
    # from jobs.examples.ex3_incremental_job import Job as Ex3_Incremental_Job

    # dep = ex3_dependant_job.etl(sc, sc_sql, args)
    # loaded_inputs = {'processed_events':dep}
    # out = ex3_incremental_job.etl(sc, sc_sql, args, loaded_inputs)
    #-> works

    from core.etl_utils import Flow

    # myflow = Flow(sc, sc_sql, args, [Ex3_Dependant_Job, Ex3_Incremental_Job])
    myflow = Flow(sc, sc_sql, args, 'examples/ex3_incremental_job')
