"""
EMR Serverless deployment — run Spark jobs without managing clusters.

EMR Serverless auto-provisions and scales compute resources.
No cluster to create, manage, or terminate.

Key differences vs classic EMR:
  - No cluster lifecycle (no start/wait/terminate)
  - Submit a job run to an "application" (pre-created or auto-created)
  - Pay per vCPU/memory/storage per second of use
  - Simpler config: just specify spark-submit args + S3 paths
"""
import time
import json
from yaetos.logger import setup_logging
logger = setup_logging('EMRServerless')


class EMRServerlesser:
    """Deploy and monitor Spark jobs on EMR Serverless."""

    POLL_INTERVAL = 15  # seconds
    TERMINAL_STATES = {'SUCCESS', 'FAILED', 'CANCELLED'}

    @staticmethod
    def run_direct(self):
        """Submit a Spark job to EMR Serverless and optionally wait for completion."""

        # Push code package to S3
        self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])

        # Push local inputs if flagged
        if self.app_args.get('push_inputs_to_s3', False):
            from yaetos.data_sync import push_job_inputs_to_s3
            logger.info("Pushing local inputs to S3 before EMR Serverless execution...")
            push_job_inputs_to_s3(
                job_name=self.app_args.get('job_name', ''),
                jobs_metadata_file=self.app_args.get('job_param_file', 'conf/jobs_metadata.yml'),
                aws_config_file=self.deploy_args.get('aws_config_file', 'conf/aws_config.cfg'),
                aws_setup=self.deploy_args.get('aws_setup', 'dev'),
                aws_mode=self.app_args.get('mode', 'dev_EMR'),
            )

        client = self.session.client('emr-serverless')

        # Get or create application
        app_id = self.deploy_args.get('emr_serverless_app_id') or \
                 self.app_args.get('emr_serverless_app_id')

        if not app_id:
            app_id = EMRServerlesser._get_or_create_application(
                client,
                app_name=self.pipeline_name,
                spark_version=self.deploy_args.get('spark_version', '3.5'),
            )

        # Build spark-submit args
        spark_submit_params = EMRServerlesser._build_spark_submit_params(self)

        # Submit job run
        job_run_id = EMRServerlesser._submit_job_run(
            client=client,
            app_id=app_id,
            job_name=self.pipeline_name,
            execution_role=self.deploy_args.get('emr_serverless_role',
                           self.app_args.get('emr_serverless_role', '')),
            entry_point=f"s3://{self.package_path_with_bucket}/{self.app_file}",
            spark_submit_params=spark_submit_params,
            s3_log_uri=f"s3://{self.s3_bucket_logs}/{self.metadata_folder}/serverless_logs/",
        )

        logger.info(f"Job run submitted: {job_run_id}")
        logger.info(f"Application ID: {app_id}")

        # Wait and stream if requested
        if self.deploy_args.get('stream_logs', False):
            EMRServerlesser._wait_for_completion(client, app_id, job_run_id)

    @staticmethod
    def _get_or_create_application(client, app_name, spark_version='3.5'):
        """Find an existing STARTED application or create a new one."""
        # Check for existing applications
        response = client.list_applications()
        for app in response.get('applications', []):
            if app['state'] in ('CREATED', 'STARTED') and app.get('type') == 'SPARK':
                logger.info(f"Reusing existing application: {app['id']} ({app['name']}, {app['state']})")
                # Ensure it's started
                if app['state'] == 'CREATED':
                    client.start_application(applicationId=app['id'])
                    logger.info(f"Starting application {app['id']}...")
                    EMRServerlesser._wait_for_app_started(client, app['id'])
                return app['id']

        # Create new application
        emr_release = EMRServerlesser._spark_to_emr_serverless_release(spark_version)
        logger.info(f"Creating new EMR Serverless application (release: {emr_release})...")

        response = client.create_application(
            name=app_name,
            releaseLabel=emr_release,
            type='SPARK',
            autoStartConfiguration={'enabled': True},
            autoStopConfiguration={'enabled': True, 'idleTimeoutMinutes': 15},
        )
        app_id = response['applicationId']
        logger.info(f"Created application: {app_id}")

        EMRServerlesser._wait_for_app_started(client, app_id)
        return app_id

    @staticmethod
    def _spark_to_emr_serverless_release(spark_version):
        """Map Spark version to EMR Serverless release label."""
        mapping = {
            '3.3': 'emr-6.9.0',
            '3.4': 'emr-6.15.0',
            '3.5': 'emr-7.0.0',
        }
        return mapping.get(spark_version, 'emr-7.0.0')

    @staticmethod
    def _wait_for_app_started(client, app_id, timeout=300):
        """Wait for application to reach STARTED state."""
        start = time.time()
        while time.time() - start < timeout:
            response = client.get_application(applicationId=app_id)
            state = response['application']['state']
            if state == 'STARTED':
                logger.info(f"Application {app_id} is STARTED")
                return
            elif state in ('TERMINATED', 'STOPPED'):
                raise Exception(f"Application {app_id} in unexpected state: {state}")
            logger.info(f"Application {app_id}: {state}...")
            time.sleep(10)
        raise TimeoutError(f"Application {app_id} did not start within {timeout}s")

    @staticmethod
    def _build_spark_submit_params(self):
        """Build spark-submit parameters string."""
        params_parts = []

        # Add packages if needed
        packages = self.app_args.get('packages', [])
        if packages:
            params_parts.append(f"--packages {','.join(packages)}")

        # Add conf from job args
        spark_conf = self.app_args.get('spark_conf', {})
        for key, value in spark_conf.items():
            params_parts.append(f"--conf {key}={value}")

        # Add py-files (the code package)
        params_parts.append(f"--py-files s3://{self.package_path_with_bucket}/scripts.zip")

        return ' '.join(params_parts) if params_parts else ''

    @staticmethod
    def _submit_job_run(client, app_id, job_name, execution_role, entry_point,
                        spark_submit_params='', s3_log_uri=''):
        """Submit a Spark job run to EMR Serverless."""
        job_driver = {
            'sparkSubmit': {
                'entryPoint': entry_point,
            }
        }
        if spark_submit_params:
            job_driver['sparkSubmit']['sparkSubmitParameters'] = spark_submit_params

        kwargs = {
            'applicationId': app_id,
            'executionRoleArn': execution_role,
            'jobDriver': job_driver,
            'name': job_name,
        }

        if s3_log_uri:
            kwargs['configurationOverrides'] = {
                'monitoringConfiguration': {
                    's3MonitoringConfiguration': {
                        'logUri': s3_log_uri,
                    }
                }
            }

        response = client.start_job_run(**kwargs)
        return response['jobRunId']

    @staticmethod
    def _wait_for_completion(client, app_id, job_run_id):
        """Wait for job run to complete, printing status updates."""
        logger.info(f"Waiting for job run {job_run_id} to complete...")

        while True:
            response = client.get_job_run(applicationId=app_id, jobRunId=job_run_id)
            job_run = response['jobRun']
            state = job_run['state']
            state_details = job_run.get('stateDetails', '')

            logger.info(f"Job run {job_run_id}: {state}" + (f" — {state_details}" if state_details else ""))

            if state in EMRServerlesser.TERMINAL_STATES:
                if state == 'SUCCESS':
                    total_exec = job_run.get('totalExecutionDurationSeconds', 0)
                    logger.info(f"✅ Job run SUCCEEDED (execution time: {total_exec}s)")

                    # Print resource usage
                    resource_util = job_run.get('totalResourceUtilization', {})
                    if resource_util:
                        vcpu_hours = resource_util.get('vCPUHour', 0)
                        mem_gb_hours = resource_util.get('memoryGBHour', 0)
                        storage_gb_hours = resource_util.get('storageGBHour', 0)
                        logger.info(f"   Resources used: {vcpu_hours:.2f} vCPU·h, {mem_gb_hours:.2f} GB·h memory, {storage_gb_hours:.2f} GB·h storage")

                elif state == 'FAILED':
                    logger.error(f"❌ Job run FAILED")
                    if state_details:
                        logger.error(f"   Details: {state_details}")
                else:
                    logger.warning(f"⚠️  Job run ended: {state}")

                return job_run

            time.sleep(EMRServerlesser.POLL_INTERVAL)
