from yaetos.deploy import DeployPySparkScriptOnAws as Dep
import os
from pathlib import Path as Pt
import difflib
import pytest
from unittest.mock import Mock, patch
import botocore


# --- Fixtures ---
@pytest.fixture
def basic_deploy_args():
    return {
        'aws_setup': 'dev',
        'aws_config_file': 'conf/aws_config.cfg',
        'mode': 'dev_EMR',
        'deploy': 'EMR',
        'monitor_git': False
    }


@pytest.fixture
def basic_app_args():
    return {
        'job_name': 'test_job',
        'py_job': 'jobs/test_job.py',
        'mode': 'dev_EMR',
        's3_logs': 's3://test-bucket/logs/',
        'jobs_folder': 'jobs/',
        'code_source': 'repo',
        'job_param_file': 'conf/jobs_metadata.yml'
    }


@pytest.fixture
def mock_aws_config(tmp_path):
    config_content = """
[dev]
ec2_key_name = test-key
s3_region = us-east-1
user = test-user
profile_name = default
ec2_subnet_id = subnet-123
emr_ec2_role = EMR_EC2_DefaultRole
emr_role = EMR_DefaultRole
"""
    config_file = tmp_path / "aws_config.cfg"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def deployer(basic_deploy_args, basic_app_args, mock_aws_config):
    basic_deploy_args['aws_config_file'] = mock_aws_config
    return Dep(basic_deploy_args, basic_app_args)


# --- DeployPySparkScriptOnAws Tests ---
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
        test_cases = [
            ('yaetos__dev__jobs_d_some_folder_d_job__20220629T205103', 'jobs.some_folder.job'),
            ('yaetos__dev__test_s_job_d_py__20230401T123456', 'test/job.py'),
            ('yaetos__dev__my_s_folder_s_test_d_py__20230401T123456', 'my/folder/test.py'),
            ('yaetos__dev__simple_d_py__20230401T123456', 'simple.py'),
            ('invalid_pipeline_name', None),
            ('', None),
        ]
        for pipeline_name, expected in test_cases:
            job_name = Dep.get_job_name(pipeline_name)
            assert job_name == expected, f"Failed for pipeline name: {pipeline_name}"

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


def test_temp_bucket_exists_success(deployer):
    mock_s3 = Mock()
    mock_s3.meta.client.head_bucket.return_value = True

    deployer.temp_bucket_exists(mock_s3)
    mock_s3.meta.client.head_bucket.assert_called_once_with(Bucket='test-bucket')


def test_temp_bucket_exists_failure(deployer):
    mock_s3 = Mock()
    error_response = {'Error': {'Code': '404'}}
    mock_s3.meta.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        error_response, 'HeadBucket')

    with pytest.raises(SystemExit):
        deployer.temp_bucket_exists(mock_s3)


@patch('yaetos.deploy.os.path.isfile')
def test_continue_post_git_check_no_prod(mock_isfile, deployer):
    deployer.app_args['mode'] = 'dev_EMR'
    assert deployer.continue_post_git_check() is True


@patch('boto3.session.Session')
def test_s3_ops(mock_session, deployer):
    mock_s3 = Mock()
    mock_session.resource.return_value = mock_s3

    with patch.object(deployer, 'temp_bucket_exists') as mock_bucket_check, \
         patch.object(deployer, 'local_file_ops') as mock_local_ops, \
         patch.object(deployer, 'move_bash_to_local_temp') as mock_move_bash, \
         patch.object(deployer, 'upload_temp_files') as mock_upload:

        deployer.s3_ops(mock_session)

        mock_bucket_check.assert_called_once()
        mock_local_ops.assert_called_once()
        mock_move_bash.assert_called_once()
        mock_upload.assert_called_once()


@patch('yaetos.deploy.tarfile.open')
@patch('yaetos.deploy.os.listdir')
@patch('yaetos.deploy.os.walk')
def test_tar_python_scripts(mock_walk, mock_listdir, mock_tarfile, deployer, tmp_path):
    # Setup mocks
    mock_walk.return_value = [
        ('jobs', [], ['test_job.py']),
    ]
    mock_listdir.return_value = ['test_file.py']
    mock_tar = Mock()
    mock_tar.getnames.return_value = ['test_job.py', 'test_file.py']
    mock_tarfile.return_value = mock_tar

    # Create temp directory structure
    deployer.TMP = tmp_path / "tmp/files_to_ship/"
    deployer.TMP.mkdir(parents=True)

    deployer.tar_python_scripts()

    # Verify that tarfile.open was called with the correct parameters
    mock_tarfile.assert_called_with((deployer.TMP / "scripts.tar.gz"), "w:gz")

    # Verify that files were added to the tar (i.e. that add() was called at least once)
    add_calls = mock_tar.add.call_args_list
    assert len(add_calls) > 0

    # Verify specific files were added (will have more than the 2 files set above)
    added_files = [call[0][0] for call in add_calls]
    assert any('test_job.py' in str(f) for f in added_files)


# --- EMR Operations Tests ---

@patch('yaetos.deploy_emr.EMRer.run_direct')
def test_run_direct(mock_run_direct, deployer):
    deployer.run_direct()
    mock_run_direct.assert_called_once()


def test_get_active_clusters(deployer):
    mock_client = Mock()
    mock_client.list_clusters.return_value = {
        'Clusters': [
            {'Id': 'j-123', 'Name': 'Test Cluster 1', 'Status': {'State': 'WAITING'}},
            {'Id': 'j-456', 'Name': 'Test Cluster 2', 'Status': {'State': 'RUNNING'}}
        ]
    }

    clusters = deployer.get_active_clusters(mock_client)
    assert len(clusters) == 2
    assert clusters == [(1, 'j-123', 'Test Cluster 1'), (2, 'j-456', 'Test Cluster 2')]


@patch('builtins.input')
def test_choose_cluster(mock_input, deployer):
    clusters = [
        (1, 'j-123', 'Test Cluster 1'),
        (2, 'j-456', 'Test Cluster 2')
    ]

    # Test with specific cluster ID
    selected = deployer.choose_cluster(clusters, cluster_id='j-456')
    assert selected['id'] == 'j-456'

    # Test without specific cluster ID (should take first available)
    mock_input.return_value = '1'  # Simulate user entering "1"
    selected = deployer.choose_cluster(clusters)
    assert selected['id'] == 'j-123'
    mock_input.assert_called_once_with('Your choice ? ')


@patch('yaetos.deploy_emr.EMRer.start_spark_cluster')
def test_start_spark_cluster(mock_start_cluster, deployer):
    mock_client = Mock()
    mock_start_cluster.return_value = 'j-789'

    cluster_id = deployer.start_spark_cluster(mock_client, 'emr-6.1.1')
    assert cluster_id == 'j-789'
    mock_start_cluster.assert_called_once()


# --- Kubernetes Deployment Tests ---

@patch('yaetos.deploy_k8s.Kuberneter.run_direct_k8s')
def test_run_direct_k8s(mock_run_k8s, deployer):
    deployer.run_direct_k8s()
    mock_run_k8s.assert_called_once()


@patch('yaetos.deploy_k8s.Kuberneter.launch_spark_submit_k8s')
def test_launch_spark_submit_k8s(mock_launch, deployer):
    cmdline = '--master k8s:// --deploy-mode cluster'
    deployer.launch_spark_submit_k8s(cmdline)
    mock_launch.assert_called_once_with(deployer, cmdline)


# --- AWS Data Pipeline Tests ---

@patch('yaetos.deploy_aws_data_pipeline.AWS_Data_Pipeliner.run_aws_data_pipeline')
def test_run_aws_data_pipeline(mock_run_pipeline, deployer):
    deployer.run_aws_data_pipeline()
    mock_run_pipeline.assert_called_once()


def test_create_data_pipeline(deployer):
    mock_client = Mock()
    mock_client.create_pipeline.return_value = {'pipelineId': 'df-123'}

    pipe_id = deployer.create_data_pipeline(mock_client)
    assert pipe_id == 'df-123'
    mock_client.create_pipeline.assert_called_once()


@pytest.mark.skip()  # TODO: skipped since AWS DP not used. Remove code all together later.
def test_define_data_pipeline(deployer):
    mock_client = Mock()
    pipe_id = 'df-123'
    emr_instances = 2

    deployer.define_data_pipeline(mock_client, pipe_id, emr_instances)
    mock_client.put_pipeline_definition.assert_called_once()


@pytest.mark.skip()  # TODO: skipped since AWS DP not used. Remove code all together later.
def test_list_data_pipeline(deployer):
    mock_client = Mock()
    mock_client.list_pipelines.return_value = {
        'pipelineIdList': [
            {'id': 'df-123', 'name': 'test-pipeline'}
        ]
    }

    pipelines = deployer.list_data_pipeline(mock_client)
    assert len(pipelines) == 1
    assert pipelines[0]['id'] == 'df-123'


# --- Airflow Integration Tests ---

@patch('yaetos.deploy_airflow.Airflower.run_aws_airflow')
def test_run_aws_airflow(mock_run_airflow, deployer):
    deployer.run_aws_airflow()
    mock_run_airflow.assert_called_once()


def test_create_dags(deployer):
    with patch('builtins.open', create=True), \
         patch('yaetos.deploy_airflow.Airflower.create_dags') as mock_create:

        mock_create.return_value = ('local_dag.py', 'test_dag')
        fname_local, job_dag_name = deployer.create_dags()

        assert fname_local == 'local_dag.py'
        assert job_dag_name == 'test_dag'


def test_set_job_dag_name(deployer):
    job_name = 'test_job'
    dag_name = deployer.set_job_dag_name(job_name)
    assert job_name in dag_name
    assert dag_name == 'test_job_dag.py'


@patch('yaetos.deploy_airflow.Airflower.upload_dags')
def test_upload_dags(mock_upload, deployer):
    mock_s3 = Mock()
    s3_dags = 's3://airflow-dags'
    job_dag_name = 'test_dag'
    fname_local = 'local_dag.py'

    deployer.upload_dags(mock_s3, s3_dags, job_dag_name, fname_local)
    mock_upload.assert_called_once()


# --- Integration Test Examples ---

@patch.object(Dep, 's3_ops')
@patch.object(Dep, 'start_spark_cluster')
@patch.object(Dep, 'step_spark_submit')
def test_full_emr_deployment_flow(mock_submit, mock_start_cluster, mock_s3_ops, deployer):
    mock_s3 = Mock()
    mock_emr = Mock()
    mock_session = Mock()
    mock_session.resource.return_value = mock_s3
    mock_session.client.return_value = mock_emr
    deployer.session = mock_session

    # Setup EMR mock responses
    mock_emr.list_clusters.return_value = {'Clusters': []}
    mock_emr.add_job_flow_steps.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200},
        'StepIds': ['s-123']
    }
    mock_emr.run_job_flow.return_value = {
        'JobFlowId': 'j-123',
        'ResponseMetadata': {'HTTPStatusCode': 200}
    }
    deployer.cluster_id = 'j-123'  # TODO: check if this should be fixed in code start_spark_cluster()

    deployer.run_direct()

    mock_s3_ops.assert_called_once()
    mock_start_cluster.assert_called_once()
    mock_submit.assert_called_once()


@patch.object(Dep, 's3_ops')
@patch.object(Dep, 'define_data_pipeline')
@patch.object(Dep, 'activate_data_pipeline')
def test_full_data_pipeline_flow(mock_activate, mock_define, mock_s3_ops, deployer):
    mock_pipeline = Mock()
    mock_session = Mock()
    mock_session.client.return_value = mock_pipeline
    deployer.session = mock_session

    # Setup Pipeline mock responses
    mock_pipeline.create_pipeline.return_value = {'pipelineId': 'df-123'}
    mock_pipeline.list_pipelines.return_value = {
        'pipelineIdList': [
            {'id': 'df-123', 'name': 'test-pipeline'}
        ],
        'hasMoreResults': False
    }

    deployer.run_aws_data_pipeline()

    mock_s3_ops.assert_called_once()
    mock_define.assert_called_once()
    mock_activate.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])

