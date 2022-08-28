from yaetos.etl_utils import ETL_Base, Commandliner, Job_Args_Parser, Job_Yml_Parser


class Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def set_jargs(self, pre_jargs, loaded_inputs={}):
        # Function called only if running the job directly, i.e. "python jobs/generic/sql_spark_job.py --sql_file=jobs/some_job.sql", ignored if running from "python jobs/generic/launcher.py --job_name=some_job.sql"
        sql_file = pre_jargs['cmd_args']['sql_file']
        job_name = Job_Yml_Parser.set_job_name_from_file(sql_file)
        pre_jargs['job_args']['job_name'] = job_name

        sql = self.read_sql_file(sql_file)
        job_args = self.get_params_from_sql(sql)
        pre_jargs['job_args'].update(job_args)

        return Job_Args_Parser(defaults_args=pre_jargs['defaults_args'], yml_args=None, job_args=pre_jargs['job_args'], cmd_args=pre_jargs['cmd_args'], loaded_inputs=loaded_inputs)

    def transform(self, **ignored):
        sql = self.read_sql_file(self.jargs.sql_file)
        df = self.query(sql)
        if self.jargs.merged_args.get('repartition'):
            df = df.repartition(self.jargs.merged_args['repartition'])
        return df

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql

    @staticmethod
    def get_params_from_sql(sql):
        param_lines = [item.split('----')[2].split(':') for item in sql.split('\n') if (item.startswith('----param---- ') and len(item.split('----')) == 4)]
        params = {eval(key): eval(value) for key, value in param_lines}
        return params


if __name__ == "__main__":
    Commandliner(Job)