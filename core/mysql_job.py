from core.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher
from core.db_utils import pdf_to_sdf
#from libs.python_db_connectors.query_oracle import query as query_oracle
from libs.python_db_connectors.query_mysql import query as query_mysql
import core.logger as log
logger = log.setup_logging('Job')

from sqlalchemy import types


class Mysql_Job(ETL_Base):

    def query_mysql(self, query_str):
        """ Requires OUTPUT_TYPES to be specified in class.
        and mysql connection to be put in yml file in line like "api_inputs: {'api_creds': 'mysql_creds_section'}"
        output spark df."""
        # sql_file = self.args['sql_file']
        # sql = self.read_sql_file(sql_file)
        # sql = self.update_sql_file(sql)
        # self.OUTPUT_TYPES = self.get_output_types_from_sql(sql)
        # cred_profiles = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'])
        #
        # print "Running query: \n", sql
        # pdf = query_oracle(sql, db=self.db_creds, connection_type='sqlalchemy', creds_or_file=cred_profiles) # for testing locally: from libs.analysis_toolkit.query_helper import process_and_cache; pdf = process_and_cache('test', 'data/', lambda : query_oracle(sql, db=self.db_creds, connection_type='sqlalchemy', creds_or_file=cred_profiles), force_rerun=False)
        # # TODO: Check to get OUTPUT_TYPES from query_oracle, so not required here.
        # sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)

        # query_str = r"""
        #     select *
        #     FROM `notes`
        #     """.replace('%', '%%')  # replace necessary for pymysql parser
        logger.info('Query string:\n' + query_str)
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        creds_section = self.job_yml['api_inputs']['api_creds']
        pdf = query_mysql(query_str, db=creds_section, creds_or_file=creds)
        sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf


if __name__ == "__main__":
    Commandliner(Job)
