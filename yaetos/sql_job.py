from yaetos.etl_utils import ETL_Base, Commandliner, Job_Args_Parser, Job_Yml_Parser


class Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def set_jargs(self, pre_jargs, loaded_inputs={}):
        # Function called only if running the job directly, i.e. "python yaetos/sql_job.py --sql_file=jobs/some_job.sql", ignored if running from "python jobs/generic/launcher.py --job_name=some_job.sql"
        sql_file=pre_jargs['cmd_args']['sql_file']
        job_name = Job_Yml_Parser.set_job_name_from_file(sql_file)
        pre_jargs['job_args']['job_name'] = job_name
        return Job_Args_Parser(defaults_args=pre_jargs['defaults_args'], yml_args=None, job_args=pre_jargs['job_args'], cmd_args=pre_jargs['cmd_args'], loaded_inputs=loaded_inputs)

    def transform(self, **ignored):
        sql = self.read_sql_file(self.jargs.sql_file)
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
