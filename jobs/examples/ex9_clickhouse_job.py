"""Job to show code to read/write data to clickhouse using spark connector instead of using pandas/sqlalchemy.
Typically not needed since data is read/written to clickhouse from framework, as defined in job_metadata.yml.
May require VPN to access clickhouse.
"""
from core.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher
import core.logger as log
logger = log.setup_logging('Job')


class Job(ETL_Base):
    def transform(self):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        creds_section = self.jargs.yml_args['db_inputs']['creds']
        db = creds[creds_section]
        url = 'jdbc:postgresql://{host}/{service}'.format(host=db['host'], service=db['service'])
        dbtable = 'some.table'


        # Reading from clickhouse
        logger.info('Pulling table "{}" from clickhouse'.format(dbtable))
        df = self.sc_sql.read \
            .format('jdbc') \
            .option('driver', "org.postgresql.Driver") \
            .option("url", url) \
            .option("user", db['user']) \
            .option("password", db['password']) \
            .option("dbtable", dbtable)\
            .load()
        count = df.count()
        logger.info('Done pulling table, row count:{}'.format(count))
        return df


if __name__ == "__main__":
    args = {'job_param_file':   'conf/jobs_metadata.yml',
            'load_connectors':   'all',
    }
    Commandliner(Job, **args)
