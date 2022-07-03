from yaetos.deploy import DeployPySparkScriptOnAws as Dep
import yaetos.deploy
import os
from pathlib import Path as Pt
# import pytest
from unittest import mock


class Test_DeployPySparkScriptOnAws(object):

    # # using mocker (wrap around standart unittest mock lib.)
    # def test_run(self, deploy_args, app_args, mocker):
    #     # mode = 'dev_EMR'
    #     # job_name = 'jobs.some_folder.job'
    #     # user = 'n/a'
    #     deploy_args['deploy'] = 'EMR'
    #     # mocker.patch('boto3.Session', return_value=True)
    #     mocker.patch('boto3.session.Session', return_value=True)
    #     # run_direct
    #     dep = Dep(deploy_args, app_args)
    #     dep.run()

    # using mocker standart unittest mock lib
    # @mock.patch('boto3.session.Session')
    # def test_run(self, session_data, deploy_args, app_args):
    #     mock_session_obj = mock.Mock()
    #     mock_iam_client = mock.Mock()
    #     creds = mock_session_obj.get_credentials()
    #     print ('Access key: ', creds.access_key)
    #     print ('Secret key: ', creds.secret_key)
    #     print ('Region: ', mock_session_obj.region_name)
    #     print ('Profile: ', mock_session_obj.profile_name)
    #     print ('User: ', mock_iam_client.get_user())
    #     # from https://unbiased-coder.com/boto3-session-guide/#How_to_Mock_Boto3_Session
    #
    #     deploy_args['deploy'] = 'EMR'
    #     app_args['mode'] = 'dev_EMR'
    #
    #     dep = Dep(deploy_args, app_args)
    #     dep.run()
    #     # PB: botocore.exceptions.ProfileNotFound: The config profile (to_be_filled) could not be found

    # # using standart unittest mock lib
    @mock.patch('yaetos.deploy.boto3')
    def test_run(self, mocked_boto, deploy_args, app_args):

        mocked_session = mocked_boto.Session()
        mocked_client = mocked_session.resource()
        mocked_identity = mocked_client.get_caller_identity()

        # now mock the return value of .get()
        mocked_identity.get.return_value = "default_profile"

        app_args['code_source'] = 'repo'
        app_args['job_param_file'] = None
        app_args['jobs_folder'] = 'jobs/'
        app_args['mode'] = 'dev_EMR'
        deploy_args['deploy'] = 'EMR'

        dep = Dep(deploy_args, app_args)
        dep.run()
        # PB: Deploy:deploy.py:739 Could not create EMR cluster (status code <MagicMock name='boto3.Session().client().run_job_flow().__getitem__().__getitem__()' id='140032267112800'>)

    # # check using moto lib (mock_emr)
    # def test_run(self, mocked_boto, deploy_args, app_args):
    #     # https://getbetterdevops.io/how-to-mock-aws-services-in-python-unit-tests/
    #     # https://github.com/spulec/moto/issues/1925
    #     pass


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
        app_args['code_source'] = 'repo'  # TODO: other test for 'lib'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_package_path()
        expected = Pt(os.environ.get('YAETOS_FRAMEWORK_HOME', ''))
        assert actual == expected
