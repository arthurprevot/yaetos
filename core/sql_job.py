from core.etl_utils import ETL_Base, CommandLiner, Flow, GIT_REPO


class SQL_Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def commandline_launch(self, **args):
        app_name = 'test' #self.get_job_name(args)
        job_file = self.get_job_file()
        commandliner = SQLCommandLiner()
        commandliner.commandline_launch(app_name, job_file, args, etl_func=self.etl, flow_class=Flow)

    def set_attributes(self, sc, sc_sql, args):
        # ETL_Base.set_attributes(sc, sc_sql, args)
        # super(SQL_Job, self).set_attributes(sc, sc_sql, args)
        # self.job_name = self.get_job_name(args)  # differs from app_name when one spark app runs several jobs.
        # import ipdb; ipdb.set_trace()
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = sc.appName
        self.job_name = self.get_job_name(args)  # differs from app_name when one spark app runs several jobs.
        self.args = args
        self.set_job_yml()
        self.set_paths()
        self.set_is_incremental()
        self.set_frequency()


    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

    # def get_job_name(self, args):
    def get_job_name(self, args):
        # return args['sql_file'].split('/')[-1].replace('.sql','')  # TODO: redo better
        return args['sql_file'].replace(GIT_REPO+'jobs/','').replace('jobs/','').replace('.sql','')  # TODO make better with os.path functions.

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
    SQL_Job().commandline_launch(aws_setup='perso')  # aws_setup can be overriden in commandline if required
