from yaetos.deploy import DeployPySparkScriptOnAws as Dep
import os
from pathlib import Path as Pt
import difflib


def compare_files(file1_path, file2_path, verbose=False):
    with open(file1_path, 'r') as file1, open(file2_path, 'r') as file2:
        file1_lines = file1.readlines()
        file2_lines = file2.readlines()

    diff = difflib.unified_diff(file1_lines, file2_lines, fromfile=file1_path, tofile=file2_path)
    diff = list(diff)
    diff_without_ignore = [line for line in diff if line.startswith('-') and not line.strip().endswith('# ignore_in_diff') and not line.startswith('---')]

    if verbose:
        print('--- file1_lines:', ''.join(file1_lines))
        print('--- file2_lines:', ''.join(file2_lines))
        print('--- diff:', ''.join(diff))
        print('--- diff_with_ignore:', diff_without_ignore)

    if file1_lines == file2_lines:
        return True, "No diff"
    elif diff_without_ignore == []:
        return True, "No diff, except may be for line that ends with '# ignore_in_diff', i.e. paths typically ignored because of timestamps that make comparison harder."
    else:
        return False, ''.join(diff)


class Test_DeployPySparkScriptOnAws(object):
    def test_generate_pipeline_name(self):
        mode = 'n/a'  # TODO: remove need for mode param in generate_pipeline_name()
        job_name = 'jobs.some_folder.job'
        user = 'n/a'
        actual = Dep.generate_pipeline_name(mode, job_name, user)
        expected = 'yaetos__jobs_d_some_folder_d_job__20220629T205103'
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
        # deploy_args['mode'] = 'dev_EMR'
        dep = Dep(deploy_args, app_args)
        actual = dep.get_job_log_path()
        expected = 'pipelines_metadata/jobs_code/yaetos__some_job_name__20220629T211146'
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
        # Test base case
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
            '--job_name=some_job_name']
        assert actual == expected

        # Test adding args
        app_args['dependencies'] = True
        app_args['chain_dependencies'] = True
        actual = Dep.get_spark_submit_args(app_file, app_args)
        expected.insert(4, ' --dependencies --chain_dependencies')
        assert actual == expected

        # Test adding args
        app_args['sql_file'] = 'some_file.sql'
        actual = Dep.get_spark_submit_args(app_file, app_args)
        expected.insert(8, '--sql_file=/home/hadoop/app/some_file.sql')
        assert actual == expected

    def test_get_spark_submit_args_with_launcher(self, app_args):
        app_args['job_name'] = 'some_job_name'
        app_file = 'jobs/generic/launcher.py'
        actual = Dep.get_spark_submit_args(app_file, app_args)
        expected = [
            'spark-submit',
            '--verbose',
            '--py-files=/home/hadoop/app/scripts.zip',
            '/home/hadoop/app/jobs/generic/launcher.py',
            '--mode=None',
            '--deploy=none',
            '--storage=s3',
            '--job_name=some_job_name']
        assert actual == expected

    def test_get_spark_submit_k8s_docker_desktop(self, app_args):
        app_args = {
            'job_name': 'examples/ex0_extraction_job.py',
            'mode': 'dev_k8s',
            'k8s_mode': 'k8s_docker_desktop',
            'dependencies': True,
            'k8s_url': 'k8s://https://kubernetes.docker.internal:6443',
            'k8s_name': 'my-pyspark-job',
            'k8s_executor_instances': '2',
            'k8s_namespace': 'a_k8s_namespace',
            'k8s_image_service': 'a_k8s_image_service',
            'k8s_upload_path': 'a_k8s_upload_path',
            'k8s_driver_podTemplateFile': 'a_k8s_driver_podTemplateFile',
            'k8s_executor_podTemplateFile': 'a_k8s_executor_podTemplateFile',
            'aws_region': 'a_aws_region',
            'spark_deploy_args': ['--conf spark.kubernetes.driver.pod.name=a_k8s_podname'],
            'spark_app_args': []}

        app_file = 'jobs/generic/launcher.py'
        actual = Dep.get_spark_submit_args_k8s(app_file, app_args)
        expected = [  # spark-submit command that works in local k8s with docker desktop
            'spark-submit',
            '--master k8s://https://kubernetes.docker.internal:6443',
            '--deploy-mode cluster',
            '--name my-pyspark-job',
            '--conf spark.executor.instances=2',
            '--conf spark.kubernetes.namespace=a_k8s_namespace',
            '--conf spark.kubernetes.container.image=a_k8s_image_service',
            '--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-service-account',
            '--conf spark.kubernetes.pyspark.pythonVersion=3',
            '--conf spark.pyspark.python=python3',
            '--conf spark.pyspark.driver.python=python3',
            '--conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.mount.path=/mnt/yaetos_jobs',
            '--conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/',
            '--conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.mount.path=/mnt/yaetos_jobs',
            '--conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/',
            '--conf spark.kubernetes.file.upload.path=file:///yaetos_jobs/tmp/files_to_ship/scripts.zip',
            '--py-files local:///mnt/yaetos_jobs/tmp/files_to_ship/scripts.zip',
            '--conf spark.kubernetes.driver.pod.name=a_k8s_podname',
            'local:///mnt/yaetos_jobs/jobs/generic/launcher.py',
            '--mode=dev_k8s',
            '--deploy=none',
            '--storage=s3',
            '--job_name=examples/ex0_extraction_job.py',
            '--runs_on=k8s',
            '--dependencies']
        assert actual == expected

    def test_get_spark_submit_k8s_aws(self, app_args):
        app_args = {
            'job_name': 'a_job_name',
            'mode': 'dev_k8s',
            'k8s_mode': 'k8s_aws',
            'dependencies': True,
            'k8s_url': 'a_k8s_url',
            'k8s_name': 'a_k8s_name',
            'k8s_executor_instances': 'a_k8s_executor_instances',
            'k8s_namespace': 'a_k8s_namespace',
            'k8s_image_service': 'a_k8s_image_service',
            'k8s_upload_path': 'a_k8s_upload_path',
            'k8s_driver_podTemplateFile': 'a_k8s_driver_podTemplateFile',
            'k8s_executor_podTemplateFile': 'a_k8s_executor_podTemplateFile',
            'aws_region': 'a_aws_region',
            'spark_deploy_args': ['--conf spark.kubernetes.driver.pod.name=a_k8s_podname'],
            'spark_app_args': []}

        app_file = 'jobs/generic/launcher.py'
        actual = Dep.get_spark_submit_args_k8s(app_file, app_args)
        expected = [  # spark-submit command that works in k8s in AWS EKS
            'spark-submit',
            '--master a_k8s_url',
            '--deploy-mode cluster',
            '--name a_k8s_name',
            '--conf spark.executor.instances=a_k8s_executor_instances',
            '--conf spark.kubernetes.namespace=a_k8s_namespace',
            '--conf spark.kubernetes.container.image=a_k8s_image_service',
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-core:1.11.563,com.amazonaws:aws-java-sdk-s3:1.11.563',
            '--conf spark.kubernetes.file.upload.path=a_k8s_upload_path',
            '--conf spark.kubernetes.driver.podTemplateFile=a_k8s_driver_podTemplateFile',
            '--conf spark.kubernetes.executor.podTemplateFile=a_k8s_executor_podTemplateFile',
            '--conf spark.kubernetes.executor.deleteOnTermination=false',
            '--conf spark.kubernetes.container.imagePullPolicy=Always',
            '--conf spark.jars.ivy=/tmp/.ivy2',
            '--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider',
            '--conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}"',
            '--conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}"',
            '--conf spark.hadoop.fs.s3a.session.token="${AWS_SESSION_TOKEN}"',
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf spark.hadoop.fs.s3a.endpoint=s3.a_aws_region.amazonaws.com',
            '--py-files tmp/files_to_ship/scripts.zip',
            '--conf spark.kubernetes.driver.pod.name=a_k8s_podname',
            'jobs/generic/launcher.py',
            '--mode=dev_k8s',
            '--deploy=none',
            '--storage=s3',
            '--job_name=a_job_name',
            '--runs_on=k8s',
            '--dependencies']
        assert actual == expected

    def test_get_spark_submit_args_jar(self):
        app_args = {
            'jar_job': 'some/job.jar',
            'spark_app_args': 'some_arg'}
        app_file = 'jobs/generic/launcher.py'
        actual = Dep.get_spark_submit_args(app_file, app_args)
        expected = [
            'spark-submit',
            '--verbose',
            '/home/hadoop/app/some/job.jar',
            'some_arg']
        assert actual == expected

    def test_create_dags_emr(self, deploy_args, app_args):
        deploy_args['deploy'] = 'airflow'  # TODO: change to 'airflow_emr'
        app_args['local_dags'] = 'air/flow/dags/'  # TODO: move local_dags to deploy_args
        app_args['job_name'] = 'ex/job_x'
        app_args['emr_core_instances'] = 2
        app_args['s3_logs'] = 's3://mylake-dev/pipelines_metadata/manual_run_logs/'
        dep = Dep(deploy_args, app_args)
        actual_fname, actual_job_dag_name = dep.create_dags()
        actual_fname = str(actual_fname)

        expected_fname = 'air/flow/dags/ex/job_x_dag.py'
        expected_job_dag_name = 'ex/job_x_dag.py'
        assert actual_fname == expected_fname
        assert actual_job_dag_name == expected_job_dag_name
        are_equal, diff_msg = compare_files('tests/fixtures/ref_airflow_emr_job_dag.py', actual_fname, verbose=False)
        assert are_equal, f"Assert result: {are_equal}, Diff message:\n{diff_msg}"

    def test_create_dags_k8s(self, deploy_args, app_args):
        deploy_args['deploy'] = 'airflow_k8s'  # TODO: change to 'airflow_emr'
        app_args['local_dags'] = 'air/flow/dags/'  # TODO: move local_dags to deploy_args
        app_args['job_name'] = 'ex/job_x'
        dep = Dep(deploy_args, app_args)
        actual_fname, actual_job_dag_name = dep.create_dags()
        actual_fname = str(actual_fname)

        expected_fname = 'air/flow/dags/ex/job_x_dag.py'
        expected_job_dag_name = 'ex/job_x_dag.py'
        assert actual_fname == expected_fname
        assert actual_job_dag_name == expected_job_dag_name
        are_equal, diff_msg = compare_files('tests/fixtures/ref_airflow_k8s_job_dag.py', actual_fname, verbose=False)
        assert are_equal, f"Assert result: {are_equal}, Diff message:\n{diff_msg}"
