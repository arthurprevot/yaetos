from yaetos.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher
from yaetos.db_utils import pdf_to_sdf
from libs.python_db_connectors.query_mysql import query as query_mysql


class Mysql_Job(ETL_Base):

    def query_mysql(self, query_str):
        """ Requires OUTPUT_TYPES to be specified in class.
        and mysql connection to be put in yml file in line like "api_inputs: {'api_creds': 'mysql_creds_section'}"
        output spark df."""
        query_str = query_str.replace('%', '%%')  # replace necessary for pymysql parser
        self.logger.info('Query string:\n' + query_str)
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        creds_section = self.jargs.yml_args['api_inputs']['api_creds']
        self.logger.info('The query is using the credential section:' + creds_section)
        pdf = query_mysql(query_str, db=creds_section, creds_or_file=creds)
        sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf


if __name__ == "__main__":
    Commandliner(Job)
