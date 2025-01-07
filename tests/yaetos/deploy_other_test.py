import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import boto3
import botocore
from yaetos.deploy import DeployPySparkScriptOnAws
from yaetos.etl_utils import Job_Args_Parser
from yaetos.deploy_emr import EMRer
from yaetos.deploy_k8s import Kuberneter
from yaetos.deploy_aws_data_pipeline import AWS_Data_Pipeliner
from yaetos.deploy_airflow import Airflower


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
    return DeployPySparkScriptOnAws(basic_deploy_args, basic_app_args)

# --- DeployPySparkScriptOnAws Tests ---

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
    added_files = [call[0][0] for call in add_calls]  # Extract the filenames that were added
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

@patch('boto3.session.Session')
def test_full_emr_deployment_flow(mock_session, deployer):
    mock_s3 = Mock()
    mock_emr = Mock()
    mock_session.resource.return_value = mock_s3
    mock_session.client.return_value = mock_emr
    
    # Setup EMR mock responses
    mock_emr.list_clusters.return_value = {'Clusters': []}
    mock_emr.run_job_flow.return_value = {'JobFlowId': 'j-123'}
    
    with patch.object(deployer, 's3_ops') as mock_s3_ops, \
         patch.object(deployer, 'start_spark_cluster') as mock_start_cluster, \
         patch.object(deployer, 'step_spark_submit') as mock_submit:
        
        deployer.run_direct()
        
        mock_s3_ops.assert_called_once()
        mock_start_cluster.assert_called_once()
        mock_submit.assert_called_once()

@patch('boto3.session.Session')
def test_full_data_pipeline_flow(mock_session, deployer):
    mock_pipeline = Mock()
    mock_session.client.return_value = mock_pipeline
    
    # Setup Pipeline mock responses
    mock_pipeline.create_pipeline.return_value = {'pipelineId': 'df-123'}
    
    with patch.object(deployer, 's3_ops') as mock_s3_ops, \
         patch.object(deployer, 'define_data_pipeline') as mock_define, \
         patch.object(deployer, 'activate_data_pipeline') as mock_activate:
        
        deployer.run_aws_data_pipeline()
        
        mock_s3_ops.assert_called_once()
        mock_define.assert_called_once()
        mock_activate.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])