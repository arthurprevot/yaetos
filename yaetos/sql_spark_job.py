from yaetos.etl_utils import ETL_Base, Commandliner, Job_Args_Parser, Job_Yml_Parser


class Job(ETL_Base):
    """To run/deploy sql jobs, using --sql_file arg. Class reused by sql_pandas_job.sql"""

    def set_jargs(self, pre_jargs, loaded_inputs={}):
        # Function called only if running the job directly, i.e. "python jobs/generic/sql_spark_job.py --sql_file=jobs/some_job.sql",
        # ignored if running from "python jobs/generic/launcher.py --job_name=some_job.sql"
        # i.e. same behavior as other python jobs.
        sql_file = pre_jargs['cmd_args']['sql_file']
        if 'job_name' not in pre_jargs['cmd_args'].keys():
            job_name = Job_Yml_Parser.set_job_name_from_file(sql_file)
            pre_jargs['cmd_args']['job_name'] = job_name
        else:
            job_name = pre_jargs['job_args'].get('job_name')

        # Get all params from sql file and set as pre_jargs['job_args']
        query_str = self.read_sql_file(sql_file)
        job_args = self.get_params_from_sql(query_str)
        pre_jargs['job_args'].update(job_args)
        # 'job_name' param may be present in .sql file and so in pre_jargs['job_args'], but it will be overwritten by pre_jargs['cmd_args']['job_name'] if available.
        return Job_Args_Parser(defaults_args=pre_jargs['defaults_args'], yml_args=None, job_args=pre_jargs['job_args'], cmd_args=pre_jargs['cmd_args'], job_name=job_name, loaded_inputs=loaded_inputs)

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
