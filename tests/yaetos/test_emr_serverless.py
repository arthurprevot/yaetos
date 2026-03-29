"""Unit tests for EMR Serverless deployment."""
import sys
import pytest
from unittest.mock import MagicMock, patch, call

# Mock heavy dependencies
for mod_name in ["boto3", "botocore", "botocore.exceptions",
                 "networkx", "cloudpathlib", "pandas", "pyspark",
                 "dateutil", "dateutil.relativedelta",
                 "yaetos.spark_utils", "yaetos.pandas_utils",
                 "yaetos.git_utils", "yaetos.env_dispatchers",
                 "yaetos.logger"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

mock_logger = MagicMock()
sys.modules["yaetos.logger"].setup_logging = MagicMock(return_value=mock_logger)

from yaetos.deploy_emr_serverless import EMRServerlesser


class TestSparkVersionMapping:

    def test_spark_35_maps_to_emr_7(self):
        assert EMRServerlesser._spark_to_emr_serverless_release('3.5') == 'emr-7.0.0'

    def test_spark_34_maps_to_emr_615(self):
        assert EMRServerlesser._spark_to_emr_serverless_release('3.4') == 'emr-6.15.0'

    def test_spark_33_maps_to_emr_69(self):
        assert EMRServerlesser._spark_to_emr_serverless_release('3.3') == 'emr-6.9.0'

    def test_unknown_defaults_to_emr_7(self):
        assert EMRServerlesser._spark_to_emr_serverless_release('4.0') == 'emr-7.0.0'


class TestGetOrCreateApplication:

    def test_reuses_existing_started_app(self):
        client = MagicMock()
        client.list_applications.return_value = {
            'applications': [
                {'id': 'app-existing', 'name': 'test', 'state': 'STARTED', 'type': 'SPARK'},
            ]
        }

        app_id = EMRServerlesser._get_or_create_application(client, 'test-app')
        assert app_id == 'app-existing'
        client.create_application.assert_not_called()

    def test_starts_created_app(self):
        client = MagicMock()
        client.list_applications.return_value = {
            'applications': [
                {'id': 'app-created', 'name': 'test', 'state': 'CREATED', 'type': 'SPARK'},
            ]
        }
        client.get_application.return_value = {
            'application': {'state': 'STARTED'}
        }

        app_id = EMRServerlesser._get_or_create_application(client, 'test-app')
        assert app_id == 'app-created'
        client.start_application.assert_called_once_with(applicationId='app-created')

    def test_creates_new_app_when_none_exist(self):
        client = MagicMock()
        client.list_applications.return_value = {'applications': []}
        client.create_application.return_value = {'applicationId': 'app-new'}
        client.get_application.return_value = {
            'application': {'state': 'STARTED'}
        }

        app_id = EMRServerlesser._get_or_create_application(client, 'test-app', spark_version='3.5')
        assert app_id == 'app-new'
        client.create_application.assert_called_once()
        create_kwargs = client.create_application.call_args[1]
        assert create_kwargs['type'] == 'SPARK'
        assert create_kwargs['releaseLabel'] == 'emr-7.0.0'

    def test_skips_non_spark_apps(self):
        client = MagicMock()
        client.list_applications.return_value = {
            'applications': [
                {'id': 'app-hive', 'name': 'hive', 'state': 'STARTED', 'type': 'HIVE'},
            ]
        }
        client.create_application.return_value = {'applicationId': 'app-spark'}
        client.get_application.return_value = {
            'application': {'state': 'STARTED'}
        }

        app_id = EMRServerlesser._get_or_create_application(client, 'test')
        assert app_id == 'app-spark'  # Created new, didn't reuse Hive app


class TestSubmitJobRun:

    def test_basic_submission(self):
        client = MagicMock()
        client.start_job_run.return_value = {'jobRunId': 'run-123'}

        job_id = EMRServerlesser._submit_job_run(
            client=client,
            app_id='app-1',
            job_name='test-job',
            execution_role='arn:aws:iam::123:role/TestRole',
            entry_point='s3://bucket/code/job.py',
        )

        assert job_id == 'run-123'
        client.start_job_run.assert_called_once()
        kwargs = client.start_job_run.call_args[1]
        assert kwargs['applicationId'] == 'app-1'
        assert kwargs['executionRoleArn'] == 'arn:aws:iam::123:role/TestRole'
        assert kwargs['jobDriver']['sparkSubmit']['entryPoint'] == 's3://bucket/code/job.py'

    def test_submission_with_spark_params(self):
        client = MagicMock()
        client.start_job_run.return_value = {'jobRunId': 'run-456'}

        EMRServerlesser._submit_job_run(
            client=client,
            app_id='app-1',
            job_name='test',
            execution_role='arn:role',
            entry_point='s3://code/job.py',
            spark_submit_params='--packages com.example:lib:1.0 --conf spark.executor.memory=4g',
        )

        kwargs = client.start_job_run.call_args[1]
        assert 'sparkSubmitParameters' in kwargs['jobDriver']['sparkSubmit']
        assert '--packages' in kwargs['jobDriver']['sparkSubmit']['sparkSubmitParameters']

    def test_submission_with_s3_logging(self):
        client = MagicMock()
        client.start_job_run.return_value = {'jobRunId': 'run-789'}

        EMRServerlesser._submit_job_run(
            client=client,
            app_id='app-1',
            job_name='test',
            execution_role='arn:role',
            entry_point='s3://code/job.py',
            s3_log_uri='s3://logs/serverless/',
        )

        kwargs = client.start_job_run.call_args[1]
        assert 'configurationOverrides' in kwargs
        log_conf = kwargs['configurationOverrides']['monitoringConfiguration']['s3MonitoringConfiguration']
        assert log_conf['logUri'] == 's3://logs/serverless/'


class TestWaitForCompletion:

    def test_immediate_success(self):
        client = MagicMock()
        client.get_job_run.return_value = {
            'jobRun': {
                'state': 'SUCCESS',
                'totalExecutionDurationSeconds': 42,
                'totalResourceUtilization': {
                    'vCPUHour': 0.5,
                    'memoryGBHour': 2.0,
                    'storageGBHour': 0.1,
                },
            }
        }

        EMRServerlesser.POLL_INTERVAL = 0
        result = EMRServerlesser._wait_for_completion(client, 'app-1', 'run-1')
        assert result['state'] == 'SUCCESS'

    def test_polls_until_success(self):
        client = MagicMock()
        client.get_job_run.side_effect = [
            {'jobRun': {'state': 'SUBMITTED'}},
            {'jobRun': {'state': 'RUNNING'}},
            {'jobRun': {'state': 'SUCCESS', 'totalExecutionDurationSeconds': 60, 'totalResourceUtilization': {}}},
        ]

        EMRServerlesser.POLL_INTERVAL = 0
        result = EMRServerlesser._wait_for_completion(client, 'app-1', 'run-1')
        assert result['state'] == 'SUCCESS'
        assert client.get_job_run.call_count == 3

    def test_handles_failure(self):
        client = MagicMock()
        client.get_job_run.return_value = {
            'jobRun': {
                'state': 'FAILED',
                'stateDetails': 'User application exited with status 1',
            }
        }

        EMRServerlesser.POLL_INTERVAL = 0
        result = EMRServerlesser._wait_for_completion(client, 'app-1', 'run-1')
        assert result['state'] == 'FAILED'
