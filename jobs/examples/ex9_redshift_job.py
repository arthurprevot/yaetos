"""Job to show code to read/write data to redshift. Typical not needed since data is read/written to redshift from framework, as defined in job_metadata.yml.
Requires VPN to access redshift.
"""
from core.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher, REDSHIFT_S3_TMP_DIR
import core.logger as log
logger = log.setup_logging('Job')


class Job(ETL_Base):
    def transform(self, some_events):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        creds_section = self.job_yml['copy_to_redshift']['creds']
        db = creds[creds_section]
        url = 'jdbc:redshift://{host}:{port}/{service}'.format(host=db['host'], port=db['port'], service=db['service'])
        dbtable = 'sandbox.test_ex9_redshift'

        # Writing to redshift
        logger.info('Sending table "{}" to redshift, size "{}".'.format(dbtable, some_events.count()))
        some_events.write \
            .format('com.databricks.spark.redshift') \
            .option("tempdir", REDSHIFT_S3_TMP_DIR) \
            .option("url", url) \
            .option("user", db['user']) \
            .option("password", db['password']) \
            .option("dbtable", dbtable) \
            .mode('overwrite') \
            .save()
        logger.info('Done sending table')

        # Reading from redshift
        logger.info('Pulling table "{}" from redshift'.format(dbtable))
        df = self.sc_sql.read \
            .format('com.databricks.spark.redshift') \
            .option("tempdir", REDSHIFT_S3_TMP_DIR) \
            .option("url", url) \
            .option("user", db['user']) \
            .option("password", db['password']) \
            .option("dbtable", dbtable)\
            .load()
        count = df.count()
        logger.info('Done pulling table, row count:{}'.format(count))

        # Output table will also be sent to redshift as required by job_metadata.yml
        return some_events


if __name__ == "__main__":
    args = {
        'job_param_file':   'conf/jobs_metadata_local.yml',  # Just to be explicit. Not needed since this is default.
        'connection_file':  'conf/connections.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_config_file':  'conf/aws_config.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_setup':        'dev',  # Just to be explicit. Not needed since this is default.
        'jobs_folder':      'jobs/',  # Just to be explicit. Not needed since this is default.
        }
    Commandliner(Job, **args)
