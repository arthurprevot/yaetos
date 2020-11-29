from core.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def set_job_file(self):
        job_file=self.jargs.cmd_args['sql_file']
        # logger.info("job_file: '{}'".format(job_file))
        return job_file

    def transform(self, **ignored):
        sql = self.read_sql_file(self.jargs.cmd_args['sql_file'])
        df = self.query(sql)
        return df

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


if __name__ == "__main__":
    Commandliner(Job)
