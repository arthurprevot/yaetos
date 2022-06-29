from yaetos.deploy import DeployPySparkScriptOnAws as Dep


class Test_DeployPySparkScriptOnAws(object):
    def test_generate_pipeline_name(self):
        mode = 'dev_EMR'
        job_name = 'jobs.some_folder.job'
        user = 'n/a'
        actual = Dep.generate_pipeline_name(mode, job_name, user)
        expected = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        assert actual[:-15] == expected[:-15]

    def test_get_job_name(self):
        pipeline_name = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        actual = Dep.get_job_name(pipeline_name)
        expected = 'jobs.some_folder.job'
        assert actual == expected

    def test_get_job_log_path_prod(self, deploy_args, app_args):
        deploy_args = {'mode': 'prod_EMR', **deploy_args}
        dep = Dep(deploy_args, app_args)
        actual = dep.get_job_log_path()
        expected = 'pipelines_metadata/jobs_code/production'
        assert actual == expected

    def test_get_job_log_path_dev(self, deploy_args, app_args):
        deploy_args = {'mode': 'dev_EMR', **deploy_args}
        dep = Dep(deploy_args, app_args)
        actual = dep.get_job_log_path()
        expected = 'pipelines_metadata/jobs_code/yaetos__dev__some_job_name__20220629T211146'
        assert actual[:-15] == expected[:-15]
