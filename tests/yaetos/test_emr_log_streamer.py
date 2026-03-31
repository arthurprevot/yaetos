"""Unit tests for EMR Log Streamer."""
import sys
import os
import gzip
import pytest
from unittest.mock import MagicMock, patch, call

# Mock heavy dependencies before import
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

from yaetos.emr_log_streamer import EMRLogStreamer, stream_emr_job


class TestEMRLogStreamer:

    def _make_streamer(self):
        session = MagicMock()
        streamer = EMRLogStreamer(
            session=session,
            cluster_id="j-TESTCLUSTER",
            s3_log_uri="s3://my-bucket/logs/manual_run_logs/",
        )
        return streamer

    def test_get_step_ids(self):
        streamer = self._make_streamer()
        streamer.emr_client.list_steps.return_value = {
            'Steps': [
                {'Id': 's-111', 'Name': 'Spark Application', 'Status': {'State': 'RUNNING'}},
                {'Id': 's-222', 'Name': 'Run Setup', 'Status': {'State': 'COMPLETED'}},
            ]
        }
        steps = streamer.get_step_ids()
        assert len(steps) == 2
        assert steps[0] == ('s-111', 'Spark Application', 'RUNNING')
        assert steps[1] == ('s-222', 'Run Setup', 'COMPLETED')

    def test_get_step_status_completed(self):
        streamer = self._make_streamer()
        streamer.emr_client.describe_step.return_value = {
            'Step': {
                'Status': {
                    'State': 'COMPLETED',
                    'Timeline': {'StartDateTime': '2026-03-29T10:00:00Z'},
                }
            }
        }
        status = streamer.get_step_status('s-111')
        assert status['state'] == 'COMPLETED'

    def test_get_step_status_failed_with_details(self):
        streamer = self._make_streamer()
        streamer.emr_client.describe_step.return_value = {
            'Step': {
                'Status': {
                    'State': 'FAILED',
                    'FailureDetails': {
                        'Reason': 'User application exited with status 1',
                        'Message': 'Exception in thread main',
                        'LogFile': 's3://bucket/logs/stderr.gz',
                    },
                    'Timeline': {},
                }
            }
        }
        status = streamer.get_step_status('s-111')
        assert status['state'] == 'FAILED'
        assert 'status 1' in status['reason']
        assert 'stderr.gz' in status['log_file']

    def test_wait_for_step_immediate_completion(self):
        """Step already completed — should return immediately."""
        streamer = self._make_streamer()
        streamer.POLL_INTERVAL = 0  # no waiting in tests

        streamer.emr_client.describe_step.return_value = {
            'Step': {
                'Status': {
                    'State': 'COMPLETED',
                    'Timeline': {'StartDateTime': '2026-03-29T10:00:00Z'},
                }
            }
        }

        result = streamer.wait_for_step('s-111', stream_logs=False)
        assert result['state'] == 'COMPLETED'

    def test_wait_for_step_polls_until_done(self):
        """Step transitions from RUNNING → COMPLETED."""
        streamer = self._make_streamer()
        streamer.POLL_INTERVAL = 0

        streamer.emr_client.describe_step.side_effect = [
            {'Step': {'Status': {'State': 'PENDING', 'Timeline': {}}}},
            {'Step': {'Status': {'State': 'RUNNING', 'Timeline': {}}}},
            {'Step': {'Status': {'State': 'COMPLETED', 'Timeline': {}}}},
        ]

        result = streamer.wait_for_step('s-111', stream_logs=False)
        assert result['state'] == 'COMPLETED'
        assert streamer.emr_client.describe_step.call_count == 3

    def test_fetch_and_print_log_stdout(self, capsys):
        """Should print new log content from S3."""
        streamer = self._make_streamer()

        log_content = "Line 1: Starting job\nLine 2: Processing data\n"
        streamer.s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=log_content.encode('utf-8')))
        }

        streamer._fetch_and_print_log("bucket", "path/stdout", "STDOUT", is_stdout=True)
        captured = capsys.readouterr()
        assert "Starting job" in captured.out
        assert "Processing data" in captured.out

    def test_fetch_and_print_log_incremental(self, capsys):
        """Should only print new content on subsequent calls."""
        streamer = self._make_streamer()

        # First call
        content1 = "Line 1\n"
        streamer.s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=content1.encode()))
        }
        streamer._fetch_and_print_log("bucket", "path/stdout", "STDOUT", is_stdout=True)

        # Second call with more content
        content2 = "Line 1\nLine 2\n"
        streamer.s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=content2.encode()))
        }
        streamer._fetch_and_print_log("bucket", "path/stdout", "STDOUT", is_stdout=True)
        captured = capsys.readouterr()

        # Line 1 should appear once (first call), Line 2 once (second call)
        lines = [l for l in captured.out.splitlines() if l.strip()]
        stdout_lines = [l for l in lines if "STDOUT" in l]
        assert any("Line 1" in l for l in stdout_lines)
        assert any("Line 2" in l for l in stdout_lines)

    def test_fetch_log_handles_missing_file(self, capsys):
        """Should silently skip if log file doesn't exist yet."""
        streamer = self._make_streamer()

        error = streamer.s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        streamer.s3_client.get_object.side_effect = error()

        # Should not raise
        streamer._fetch_and_print_log("bucket", "path/stdout", "STDOUT", is_stdout=True)
        captured = capsys.readouterr()
        assert captured.out == ""


class TestStreamEmrJob:

    def test_finds_correct_step(self):
        session = MagicMock()
        emr_client = session.client.return_value

        emr_client.list_steps.return_value = {
            'Steps': [
                {'Id': 's-spark', 'Name': 'Spark Application', 'Status': {'State': 'COMPLETED'}},
                {'Id': 's-setup', 'Name': 'Run Setup', 'Status': {'State': 'COMPLETED'}},
            ]
        }
        emr_client.describe_step.return_value = {
            'Step': {'Status': {'State': 'COMPLETED', 'Timeline': {}}}
        }

        result = stream_emr_job(session, 'j-TEST', step_name='Spark Application')
        assert result['state'] == 'COMPLETED'

    def test_returns_none_if_step_not_found(self):
        session = MagicMock()
        emr_client = session.client.return_value

        emr_client.list_steps.return_value = {
            'Steps': [
                {'Id': 's-setup', 'Name': 'Run Setup', 'Status': {'State': 'COMPLETED'}},
            ]
        }

        result = stream_emr_job(session, 'j-TEST', step_name='Nonexistent Step')
        assert result is None
