from core.etl_utils import ETL_Base, Commandliner


class SQL_Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def __init__(self, args={}):
        self.args = args
        self.set_job_file()
        self.set_job_name(args['sql_file'])
        self.set_job_yml(args.get('meta_file'))
        self.set_paths()
        self.set_is_incremental()
        self.set_frequency()

    def transform(self, **ignored):
        sql_file = self.args['sql_file'] if self.args['storage']=='s3' else self.args['sql_file']
        sql = self.read_sql_file(sql_file)
        df = self.query(sql)
        return df

    @staticmethod
    def get_job_class(job_name):
        return SQL_Job

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


class SQLCommandliner(Commandliner):
    @staticmethod
    def define_commandline_args():
        parser = Commandliner.define_commandline_args()
        parser.add_argument("-s", "--sql_file", help="path of sql file to run")
        return parser


if __name__ == "__main__":
    SQLCommandliner(SQL_Job, aws_setup='perso')  # aws_setup can be overriden in commandline if required
