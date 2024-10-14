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
            # yml_args = Job_Yml_Parser(job_name, jargs_single_region.job_param_file, jargs_single_region.mode).yml_args
            # jargs = Job_Args_Parser(jargs_single_region.defaults_args, yml_args, jargs_single_region.job_args, jargs_single_region.cmd_args, build_yml_args=False, loaded_inputs={})

            # Job = get_job_class(yml_args['py_job'])
            jargs_single_region = Job_Args_Parser(defaults_args=args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, loaded_inputs={})
            job = JobSingleRegion(jargs=jargs_single_region, loaded_inputs={})
            # job = JobSingleRegion(pre_jargs={'defaults_args': {}, 'job_args': jargs_single_region.merged_args, 'cmd_args': {}}, loaded_inputs={})
            
            # import ipdb; ipdb.set_trace()
            dfs[args['job_name']] = job.etl(self.sc, self.sc_sql)

        return None


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
