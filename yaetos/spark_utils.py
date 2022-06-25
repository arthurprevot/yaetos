"""
Helper functions for spark ops.
"""
from yaetos.logger import setup_logging
logger = setup_logging('Job')
try:
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType
    SPARK_SETUP = True
except ModuleNotFoundError or ImportError:
    logger.debug('Yaetos will work in pandas mode only or to push jobs to AWS, since pyspark is not found.')
    SPARK_SETUP = False


def identify_non_unique_pks(df, pks):
    windowSpec = Window.partitionBy([F.col(item) for item in pks])
    df = df.withColumn('_count_pk', F.count('*').over(windowSpec)) \
        .where(F.col('_count_pk') >= 2)
    # Debug: df.repartition(1).write.mode('overwrite').option("header", "true").csv('data/sandbox/non_unique_test/')
    return df


def add_created_at(sdf, start_dt):
    return sdf.withColumn('_created_at', F.lit(start_dt))


def create_empty_sdf(sc_sql):
    return sc_sql.createDataFrame([], StructType([]))


if __name__ == '__main__':
    pass
