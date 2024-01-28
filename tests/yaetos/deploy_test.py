from yaetos.deploy import DeployPySparkScriptOnAws as Dep
import os
from pathlib import Path as Pt


class Test_DeployPySparkScriptOnAws(object):
    def test_generate_pipeline_name(self):
        mode = 'dev_EMR'
        job_name = 'jobs.some_folder.job'
        user = 'n/a'
        actual = Dep.generate_pipeline_name(mode, job_name, user)
        expected = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        assert actual[:-15] == expected[:-15]  # [:-15] to remove timestamp

    def test_get_job_name(self):
        pipeline_name = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        actual = Dep.get_job_name(pipeline_name)
        expected = 'jobs.some_folder.job'
        assert actual == expected

    def test_get_job_log_path_prod(self, deploy_args, app_args):
        deploy_args['mode'] = 'prod_EMR'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_job_log_path()
        expected = 'pipelines_metadata/jobs_code/production'
        assert actual == expected

    def test_get_job_log_path_dev(self, deploy_args, app_args):
        deploy_args['mode'] = 'dev_EMR'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_job_log_path()
        expected = 'pipelines_metadata/jobs_code/yaetos__dev__some_job_name__20220629T211146'
        assert actual[:-15] == expected[:-15]  # [:-15] to remove timestamp

    def test_get_package_path(self, deploy_args, app_args):
        # Test 'repo' option
        app_args['code_source'] = 'repo'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_package_path()
        expected = Pt(os.environ.get('YAETOS_FRAMEWORK_HOME', ''))
        assert actual == expected

        # Test 'dir' option
        app_args['code_source'] = 'dir'
        app_args['code_source_path'] = 'some/path/'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_package_path()
        expected = Pt('some/path/')
        assert actual == expected
        # TODO: other test for 'lib'

    def test_get_spark_submit_args(self, app_args):
        app_args['mode'] = 'mode_x'
        app_file = 'some_file.py'
        actual = Dep.get_spark_submit_args(app_file, app_args)
        expected = [
            'spark-submit',
            '--verbose',
            '--py-files=/home/hadoop/app/scripts.zip',
            '/home/hadoop/app/some_file.py',
            '--mode=mode_x',
            '--deploy=none',
            '--storage=s3',
            '--rerun_criteria=None',
            '--job_name=some_job_name',
            ]
        assert actual == expected

    def test_get_spark_submit_args2(self, app_args):
        # Test base case
        app_args['mode'] = 'mode_x'
        app_file = 'some_file.py'
        actual = Dep.get_spark_submit_args2(app_file, app_args)
        expected = [
            'spark-submit',
            '--verbose',
            '--py-files=/home/hadoop/app/scripts.zip',
            '/home/hadoop/app/some_file.py',
            '--mode=mode_x',
            '--deploy=none',
            '--storage=s3',
            '--job_name=some_job_name',
            ]
        assert actual == expected

        # Test adding args
        app_args['dependencies'] = True
        app_args['chain_dependencies'] = True
        actual = Dep.get_spark_submit_args2(app_file, app_args)
        expected.insert(4, ' --dependencies --chain_dependencies')
        assert actual == expected

        # Test adding args
        app_args['sql_file'] = 'some_file.sql'
        actual = Dep.get_spark_submit_args2(app_file, app_args)
        expected.insert(8, '--sql_file=/home/hadoop/app/some_file.sql')
        assert actual == expected

    def test_get_spark_submit_args_jar(self):
        app_args = {
            'jar_job': 'some/job.jar',
            'spark_app_args': 'some_arg',
            }
        app_file = app_args['jar_job']
        actual = Dep.get_spark_submit_args2(app_file, app_args)
        expected = [
            'spark-submit',
            '--verbose',
            '/home/hadoop/app/some/job.jar',
            'some_arg',
            ]
        assert actual == expected