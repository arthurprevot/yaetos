"""Helper functions for redshift."""

from yaetos.logger import setup_logging
logger = setup_logging('Redshift')


def create_table(df, connection_profile, name_tb, schema, creds_or_file, is_incremental, s3_tmp_dir, spark_version):
    """
    Creates table in redshift, full drop or incremental drop, using spark connector. Implies pushing data to S3 first.
    """
    load_type = 'overwrite' if not is_incremental else 'append'
    db = creds_or_file[connection_profile]
    url = 'jdbc:redshift://{host}:{port}/{service}'.format(host=db['host'], port=db['port'], service=db['service'])
    dbtable = '{}.{}'.format(schema, name_tb)

    logger.info('Sending table "{}" to redshift in schema "{}", load type "{}", size "{}".'.format(name_tb, schema, load_type, df.count()))
    format = 'io.github.spark_redshift_community.spark.redshift' if spark_version == '3.1' else 'com.databricks.spark.redshift'  # else implies spark_version == '2.4'

    df.write \
        .format(format) \
        .option("tempdir", s3_tmp_dir) \
        .option("url", url) \
        .option("forward_spark_s3_credentials", "true") \
        .option("user", db['user']) \
        .option("password", db['password']) \
        .option("dbtable", dbtable) \
        .mode(load_type) \
        .save()

    logger.info("Copied table to redshift '{}.{}', using connection profile '{}'".format(schema, name_tb, connection_profile))
