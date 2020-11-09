"""Helper functions for redshift."""

# from libs.python_db_connectors.query_redshift import connect
import core.logger as log
logger = log.setup_logging('Redshift')
# import numpy as np


def create_table(df, connection_profile, name_tb, schema, creds_or_file, is_incremental):
    """
    Creates table in redshift, full drop or incremental drop, using spark connector. Implies pushing data to S3 first.
    """
    load_type = 'overwrite' if not is_incremental else 'append'
    tempdir = "s3a://sandbox-arthur/yaetos/tmp_spark/"

    logger.info('Sending table "{}" to redshift in schema "{}", load type "{}", size "{}".'.format(name_tb, schema, load_type, df.count()))

    user = creds_or_file[connection_profile]['user']
    password = creds_or_file[connection_profile]['password']
    host = creds_or_file[connection_profile]['host']
    port = creds_or_file[connection_profile]['port']
    service = creds_or_file[connection_profile]['service']
    url = 'jdbc:redshift://{host}:{port}/{service}'.format(host=host, port=port, service=service)
    dbtable = '{}.{}'.format(schema, name_tb)
    df.write \
        .format('com.databricks.spark.redshift') \
        .option("tempdir", tempdir) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", dbtable) \
        .mode(load_type) \
        .save()

    logger.info("Copied table to redshift '{}.{}', using connection profile '{}'".format(schema, name_tb, connection_profile))
