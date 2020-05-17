from pyspark import SparkContext
from pyspark.sql import SQLContext


def query_sparksql_local(query_str, fnames_in=None, dfs_in=None, **kwargs):
    """Slower than query_pandasql.py but supports windowing functions."""
    sc = SparkContext("local", "local_run")
    sc_sql = SQLContext(sc)
    run_sql = sc_sql.sql

    if fnames_in:
        for name, fname in fnames_in.items():
            reader = sc_sql.read.option("header", "true")
            if kwargs.get('delimiter'):
                reader = reader.option("delimiter", kwargs['delimiter'])
            # if kwargs.get('quoting'):  # function tried (using jobisjob_apipull_missing_advertisers_with_lkin_process.py) but didn't work. TODO: get it working.
            #     reader = reader.option('quote', '"').option('escape', '"')
            df = reader.csv(fname)
            df.createOrReplaceTempView(name)
            print("Input {}, data types: {}".format(name, [(fd.name, fd.dataType) for fd in df.schema.fields]))

    if dfs_in:
        ## Input dfs may require enforced standard types in pandas dfs (no "object"), so they are compatible with spark dfs.
        for name, df in dfs_in.items():
            df = sc_sql.createDataFrame(df)
            df.createOrReplaceTempView(name)
            print("Input {}, data types: {}".format(name, [(fd.name, fd.dataType) for fd in df.schema.fields]))

    df_out = run_sql(query_str)
    df_out.cache()
    print('Sample output'.format(df_out.show()))  # TODO: rely on upstream functions arg 'show' when done before dropping.
    pdf = df_out.toPandas()
    sc.stop()
    return pdf
