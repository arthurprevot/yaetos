"""Helper functions for clickhouse. Using postgres connector."""

from yaetos.logger import setup_logging
logger = setup_logging('Clickhouse')


def create_table(df, connection_profile, name_tb, schema, creds_or_file, is_incremental):
    """
    Creates table in Clickhouse, full drop or incremental drop, using spark connector. Implies pushing data to S3 first.
    """
    load_type = 'overwrite' if not is_incremental else 'append'
    db = creds_or_file[connection_profile]
    url = 'jdbc:postgresql://{host}/{service}'.format(host=db['host'], service=db['service'])
    dbtable = '{}.{}'.format(schema, name_tb)

    logger.info('Sending table "{}" to clickhouse in schema "{}", load type "{}", size "{}".'.format(name_tb, schema, load_type, df.count()))

    df.write \
        .format('jdbc') \
        .option('driver', "org.postgresql.Driver") \
        .option("url", url) \
        .option("user", db['user']) \
        .option("password", db['password']) \
        .option("dbtable", dbtable)\
        .mode(load_type) \
        .save()

    logger.info("Copied table to Clickhouse '{}.{}', using connection profile '{}'".format(schema, name_tb, connection_profile))
