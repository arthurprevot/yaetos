"""
EMR Log Streamer — Stream step logs from AWS EMR to the local terminal.

Polls EMR step status and fetches stdout/stderr logs from S3 in near real-time,
so you can monitor remote job execution without checking the AWS console.
"""
import time
import boto3
from yaetos.logger import setup_logging
logger = setup_logging('EMRLogStreamer')


class EMRLogStreamer:
    """Monitor an EMR step and stream its logs to the terminal."""

    POLL_INTERVAL = 15  # seconds between status checks
    LOG_POLL_INTERVAL = 10  # seconds between log fetches
    TERMINAL_STATES = {'COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'}

    def __init__(self, session, cluster_id, s3_log_uri=None):
        """
        Args:
            session: boto3 Session
            cluster_id: EMR cluster ID
            s3_log_uri: S3 URI where EMR writes logs (e.g. s3://bucket/path/manual_run_logs/)
        """
        self.emr_client = session.client('emr')
        self.s3_client = session.client('s3')
        self.cluster_id = cluster_id
        self.s3_log_uri = s3_log_uri
        self._stdout_offset = 0
        self._stderr_offset = 0

    def get_step_ids(self):
        """Get all step IDs for this cluster, most recent first."""
        response = self.emr_client.list_steps(ClusterId=self.cluster_id)
        return [(s['Id'], s['Name'], s['Status']['State']) for s in response['Steps']]

    def get_step_status(self, step_id):
        """Get current status of a step."""
        response = self.emr_client.describe_step(
            ClusterId=self.cluster_id,
            StepId=step_id,
        )
        step = response['Step']
        status = step['Status']
        return {
            'state': status['State'],
            'reason': status.get('FailureDetails', {}).get('Reason', ''),
            'message': status.get('FailureDetails', {}).get('Message', ''),
            'log_file': status.get('FailureDetails', {}).get('LogFile', ''),
            'timeline': status.get('Timeline', {}),
        }

    def wait_for_step(self, step_id, stream_logs=True):
        """
        Wait for a step to complete, streaming logs to terminal.

        Args:
            step_id: EMR step ID to monitor
            stream_logs: If True, fetch and print logs from S3

        Returns:
            dict with final step status
        """
        logger.info(f"Monitoring step {step_id} on cluster {self.cluster_id}")
        logger.info(f"Poll interval: {self.POLL_INTERVAL}s")

        last_log_fetch = 0

        while True:
            status = self.get_step_status(step_id)
            state = status['state']

            # Print status update
            timeline = status.get('timeline', {})
            start = timeline.get('StartDateTime', '')
            logger.info(f"Step {step_id}: {state}" + (f" (started: {start})" if start else ""))

            # Stream logs if available
            if stream_logs and self.s3_log_uri and (time.time() - last_log_fetch) >= self.LOG_POLL_INTERVAL:
                self._stream_step_logs(step_id)
                last_log_fetch = time.time()

            # Check terminal state
            if state in self.TERMINAL_STATES:
                # Final log fetch
                if stream_logs and self.s3_log_uri:
                    time.sleep(5)  # Wait for final logs to be written
                    self._stream_step_logs(step_id)

                if state == 'COMPLETED':
                    logger.info(f"✅ Step {step_id} COMPLETED successfully")
                elif state == 'FAILED':
                    logger.error(f"❌ Step {step_id} FAILED")
                    if status['reason']:
                        logger.error(f"   Reason: {status['reason']}")
                    if status['message']:
                        logger.error(f"   Message: {status['message']}")
                    if status['log_file']:
                        logger.error(f"   Log file: {status['log_file']}")
                else:
                    logger.warning(f"⚠️  Step {step_id} ended with state: {state}")

                return status

            time.sleep(self.POLL_INTERVAL)

    def _stream_step_logs(self, step_id):
        """Fetch stdout and stderr logs from S3 and print new content."""
        if not self.s3_log_uri:
            return

        # EMR log path format: s3://bucket/path/cluster_id/steps/step_id/
        log_base = self.s3_log_uri.rstrip('/')
        # Handle both s3:// and s3a:// prefixes
        log_base = log_base.replace('s3a://', 's3://').replace('s3n://', 's3://')
        step_log_prefix = f"{log_base}/{self.cluster_id}/steps/{step_id}/"

        # Parse bucket and prefix
        parts = step_log_prefix.replace('s3://', '').split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        # Fetch stdout
        self._fetch_and_print_log(bucket, f"{prefix}stdout.gz", "STDOUT", is_stdout=True)
        self._fetch_and_print_log(bucket, f"{prefix}stderr.gz", "STDERR", is_stdout=False)

        # Also try non-gzipped versions
        self._fetch_and_print_log(bucket, f"{prefix}stdout", "STDOUT", is_stdout=True)
        self._fetch_and_print_log(bucket, f"{prefix}stderr", "STDERR", is_stdout=False)

    def _fetch_and_print_log(self, bucket, key, label, is_stdout=True):
        """Fetch a log file from S3 and print any new content since last fetch."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            body = response['Body'].read()

            # Handle gzipped content
            if key.endswith('.gz'):
                import gzip
                body = gzip.decompress(body)

            content = body.decode('utf-8', errors='replace')
            offset = self._stdout_offset if is_stdout else self._stderr_offset

            if len(content) > offset:
                new_content = content[offset:]
                # Print with prefix
                for line in new_content.splitlines():
                    if line.strip():
                        prefix = "📋" if is_stdout else "⚠️ "
                        print(f"  {prefix} [{label}] {line}")

                if is_stdout:
                    self._stdout_offset = len(content)
                else:
                    self._stderr_offset = len(content)

        except self.s3_client.exceptions.NoSuchKey:
            pass  # Log file not yet available
        except Exception as e:
            if 'NoSuchKey' not in str(e) and 'Not Found' not in str(e):
                logger.debug(f"Could not fetch {label} log: {e}")


def stream_emr_job(session, cluster_id, s3_log_uri=None, step_name='Spark Application'):
    """
    Convenience function: find the latest step matching step_name and stream its logs.

    Args:
        session: boto3 Session
        cluster_id: EMR cluster ID
        s3_log_uri: S3 base path for logs
        step_name: Name of the step to monitor (default: 'Spark Application')

    Returns:
        dict with final step status, or None if step not found
    """
    streamer = EMRLogStreamer(session, cluster_id, s3_log_uri)

    # Find the step
    steps = streamer.get_step_ids()
    logger.info(f"Steps on cluster {cluster_id}:")
    for sid, name, state in steps:
        logger.info(f"  {sid}: {name} [{state}]")

    # Find the target step (most recent matching name)
    target_step = None
    for sid, name, state in steps:
        if step_name.lower() in name.lower():
            target_step = sid
            break

    if not target_step:
        logger.warning(f"No step matching '{step_name}' found")
        return None

    logger.info(f"Streaming logs for step: {target_step}")
    return streamer.wait_for_step(target_step)
