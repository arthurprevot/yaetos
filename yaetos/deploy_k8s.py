import os
from yaetos.logger import setup_logging
from yaetos.airflow_template_k8s import get_template_k8s
logger = setup_logging('Deploy')


class Kuberneter():
    def run_direct_k8s(self):
        """Useful to run job on cluster on the spot, without going through scheduler."""
        self.local_file_ops()
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        logger.info("Sending spark-submit to k8s cluster")
        logger.info("To monitor progress:")
        logger.info(f"kubectl get pods -n {self.app_args['k8s_namespace']}")
        logger.info(f"kubectl logs your_spark_app_id_here -n {self.app_args['k8s_namespace']}")
        cmdline = self.get_spark_submit_args_k8s(self.app_file, self.app_args)
        self.launch_spark_submit_k8s(cmdline)
        logger.info("Spark submit finished, see results with:")
        logger.info(f"kubectl get pods -n {self.app_args['k8s_namespace']}")
        logger.info(f"kubectl logs your_spark_app_id_here -n {self.app_args['k8s_namespace']}")

    @staticmethod
    def get_spark_submit_args_k8s(app_file, app_args):
        """ app_file is launcher, might be py_job too, but may also be separate from py_job (ex python launcher.py --job_name=some_job_with_py_job)."""
        # TODO: need to unify with get_spark_submit_args(), to account for jar jobs, to use eu.Runner.create_spark_submit()
        # See spark-submit setup for k8s on docker_desktop.

        if app_args.get('spark_submit'):
            return app_args['spark_submit']

        # For k8s in AWS, for yaetos jobs.
        spark_submit_base = [
            'spark-submit',
            f'--master {app_args["k8s_url"]}',
            '--deploy-mode cluster',
            f'--name {app_args["k8s_name"]}',
            f'--conf spark.executor.instances={app_args["k8s_executor_instances"]}',
            f'--conf spark.kubernetes.namespace={app_args["k8s_namespace"]}',
            f'--conf spark.kubernetes.container.image={app_args["k8s_image_service"]}']

        spark_submit_aws = [
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-core:1.11.563,com.amazonaws:aws-java-sdk-s3:1.11.563',
            f'--conf spark.kubernetes.file.upload.path={app_args["k8s_upload_path"]}',
            f'--conf spark.kubernetes.driver.podTemplateFile={app_args["k8s_driver_podTemplateFile"]}',
            f'--conf spark.kubernetes.executor.podTemplateFile={app_args["k8s_executor_podTemplateFile"]}',
            '--conf spark.kubernetes.executor.deleteOnTermination=false',
            '--conf spark.kubernetes.container.imagePullPolicy=Always',
            '--conf spark.jars.ivy=/tmp/.ivy2',
            '--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider',
            '--conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}"',
            '--conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}"',
            '--conf spark.hadoop.fs.s3a.session.token="${AWS_SESSION_TOKEN}"',
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            f'--conf spark.hadoop.fs.s3a.endpoint=s3.{app_args["aws_region"]}.amazonaws.com',
            '--py-files tmp/files_to_ship/scripts.zip']

        spark_submit_docker_desktop = [
            '--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-service-account',
            '--conf spark.kubernetes.pyspark.pythonVersion=3',
            '--conf spark.pyspark.python=python3',
            '--conf spark.pyspark.driver.python=python3',
            '--conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.mount.path=/mnt/yaetos_jobs',
            '--conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/',
            '--conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.mount.path=/mnt/yaetos_jobs',
            '--conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/',
            '--conf spark.kubernetes.file.upload.path=file:///yaetos_jobs/tmp/files_to_ship/scripts.zip',
            '--py-files local:///mnt/yaetos_jobs/tmp/files_to_ship/scripts.zip']

        if app_args.get('k8s_mode') == 'k8s_aws':
            launcher = 'jobs/generic/launcher.py'
            spark_submit_conf_plateform = spark_submit_aws
        elif app_args.get('k8s_mode') == 'k8s_docker_desktop':
            launcher = 'local:///mnt/yaetos_jobs/jobs/generic/launcher.py'
            spark_submit_conf_plateform = spark_submit_docker_desktop
        else:
            raise Exception(f"k8s_mode not recognized, should be 'k8s_aws' or 'k8s_docker_desktop'. Set to {app_args.get('k8s_mode')}")

        spark_submit_conf_extra = app_args.get('spark_deploy_args', [])
        spark_submit_jobs = [
            launcher,
            f'--mode={app_args["mode"]}',  # need to make sure it uses a mode compatible with k8s
            '--deploy=none',
            '--storage=s3',
            f'--job_name={app_args["job_name"]}',
            '--runs_on=k8s']

        spark_submit_jobs_extra = app_args.get('spark_app_args', [])
        if app_args.get('dependencies'):
            spark_submit_jobs_extra += ['--dependencies']

        spark_submit = spark_submit_base \
            + spark_submit_conf_plateform \
            + spark_submit_conf_extra \
            + spark_submit_jobs \
            + spark_submit_jobs_extra
        return spark_submit

    def launch_spark_submit_k8s(self, cmdline):
        cmdline_str = " ".join(cmdline)
        logger.info(f'About to run spark submit command line (for reuse): {cmdline_str}')
        logger.info('About to run spark submit command line (for visual check): \n{}'.format(" \n".join(cmdline)))
        if not self.app_args.get('dry_run'):
            os.system(cmdline_str)

    def get_airflow_code(self):
        params = {
            'k8s_url': self.deploy_args.get('k8s_url'),
            'k8s_name': self.deploy_args.get('k8s_name'),
            'k8s_executor_instances': self.deploy_args.get('k8s_executor_instances'),
            'k8s_namespace': self.deploy_args.get('k8s_namespace'),
            'k8s_image_service': self.deploy_args.get('k8s_image_service'),
            'k8s_upload_path': self.deploy_args.get('k8s_upload_path'),
            'k8s_driver_podTemplateFile': self.deploy_args.get('k8s_driver_podTemplateFile'),
            'k8s_executor_podTemplateFile': self.deploy_args.get('k8s_executor_podTemplateFile'),
            'aws_region': self.s3_region,
            'k8s_podname': self.deploy_args.get('k8s_podname'),
            'k8s_airflow_spark_submit_yaml': self.deploy_args.get('k8s_airflow_spark_submit_yaml'),
            # airflow specific
            'dag_nameid': self.app_args['job_name'].replace("/", "-"),
            'start_date': start_date,
            'schedule': schedule,
            'emails': self.deploy_args.get('emails', '[]'),
        }
        param_extras = {key: self.deploy_args[key] for key in self.deploy_args if key.startswith('airflow.')}
        content = get_template_k8s(params, param_extras)
        return content
