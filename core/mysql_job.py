from core.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher
from core.db_utils import pdf_to_sdf
from libs.python_db_connectors.query_mysql import query as query_mysql
import core.logger as log
logger = log.setup_logging('Job')


class Mysql_Job(ETL_Base):

    def query_mysql(self, query_str):
        """ Requires OUTPUT_TYPES to be specified in class.
        and mysql connection to be put in yml file in line like "api_inputs: {'api_creds': 'mysql_creds_section'}"
        output spark df."""
        logger.info('Query string:\n' + query_str)
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        creds_section = self.job_yml['api_inputs']['api_creds']
        pdf = query_mysql(query_str, db=creds_section, creds_or_file=creds)
        sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf


if __name__ == "__main__":
    Commandliner(Job)
