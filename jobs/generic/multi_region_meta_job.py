from yaetos.etl_utils import ETL_Base, Commandliner, get_job_class, Job_Args_Parser


class Job(ETL_Base):
    def transform(self, **kwargs):
        py_job_single_region = self.jargs.merged_args['py_job_individual']
        JobSingleRegion = get_job_class(py_job_single_region)
        dfs = {}
        for region in self.jargs.regions_to_loop:
            self.logger.info(f"--- About to run job for region={region} ---")

            args = self.jargs.merged_args
            args['job_name'] = f'{self.jargs.job_name}_{region}'
            args['region'] = region
            args['inputs'] = args['inputs_region']
            args['output'] = args['output_region']
            jargs_single_region = Job_Args_Parser(defaults_args=args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, loaded_inputs={})
            job = JobSingleRegion(jargs=jargs_single_region, loaded_inputs={})
            dfs[args['job_name']] = job.etl(self.sc, self.sc_sql)

        return None


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
