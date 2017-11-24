from core.etl_utils import ETL_Base, CommandLiner


class SQL_Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def __init__(self, args={}):
        self.args = args
        self.set_job_file()
        self.set_job_name(args['sql_file'])
        self.set_job_yml(self.job_name)
        self.set_paths()
        self.set_is_incremental()
        self.set_frequency()

    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

    # @staticmethod
    # def get_job_name(sql_file):
    #     return sql_file.replace(GIT_REPO+'jobs/','').replace('jobs/','')

    @staticmethod
    def get_job_class(job_name):
        return SQL_Job

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


class SQLCommandLiner(CommandLiner):
    @staticmethod
    def define_commandline_args():
        parser = CommandLiner.define_commandline_args()
        parser.add_argument("-s", "--sql_file", help="path of sql file to run")
        return parser


if __name__ == "__main__":
    SQLCommandLiner(SQL_Job, aws_setup='perso')  # aws_setup can be overriden in commandline if required
