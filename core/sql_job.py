from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def set_job_file(self):
        job_file=self.args['sql_file']
        # logger.info("job_file: '{}'".format(job_file))
        return job_file

    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

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
        parser.add_argument("-q", "--sql_file", help="path of sql file to run")
        return parser


if __name__ == "__main__":
    SQLCommandliner(Job)
