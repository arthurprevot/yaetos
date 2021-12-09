from yaetos.etl_utils import ETL_Base, Commandliner, Cred_Ops_Dispatcher
from yaetos.db_utils import pdf_to_sdf
from libs.python_db_connectors.query_oracle import query as query_oracle
from sqlalchemy import types


class Job(ETL_Base):
    """To run/deploy sql jobs, requires --sql_file arg."""

    def set_job_file(self):
        job_file=self.jargs.cmd_args['sql_file']
        # logger.info("job_file: '{}'".format(job_file))
        return job_file

    def transform(self, **ignored):
        sql_file = self.jargs.cmd_args['sql_file']
        sql = self.read_sql_file(sql_file)
        sql = self.update_sql_file(sql)
        self.OUTPUT_TYPES = self.get_output_types_from_sql(sql)
        cred_profiles = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage)

        print "Running query: \n", sql
        pdf = query_oracle(sql, db=self.db_creds, connection_type='sqlalchemy', creds_or_file=cred_profiles) # for testing locally: from libs.analysis_toolkit.query_helper import process_and_cache; pdf = process_and_cache('test', 'data/', lambda : query_oracle(sql, db=self.db_creds, connection_type='sqlalchemy', creds_or_file=cred_profiles), force_rerun=False)
        # TODO: Check to get OUTPUT_TYPES from query_oracle, so not required here.
        sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql

    def update_sql_file(self, sql):
        for var_name, table_name in self.INPUTS.iteritems():
            sql = sql.replace(var_name+' ', table_name+' ')  # TODO: don't require extra space.
        return sql

    @staticmethod
    def get_output_types_from_sql(sql):
        type_lines = [item.split('-----')[1].split(':') for item in sql.split('\n') if item.startswith('----- ')]
        output_types = {eval(item[0]):eval('types.'+item[1]) for item in type_lines}
        return output_types


class SQLCommandliner(Commandliner):
    @staticmethod
    def define_commandline_args():
        parser = Commandliner.define_commandline_args()
        parser.add_argument("-q", "--sql_file", help="path of sql file to run")
        return parser


if __name__ == "__main__":
    SQLCommandliner(Job)
