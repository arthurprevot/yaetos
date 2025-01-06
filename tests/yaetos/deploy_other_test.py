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

def test_generate_pipeline_name():
    """Test pipeline name generation"""
    name = DeployPySparkScriptOnAws.generate_pipeline_name('dev_EMR', 'test/job.py', 'testuser')
    assert 'yaetos__test_s_job_d_py__' in name
    assert name.startswith('yaetos__')
    assert len(name.split('__')) == 3

def test_get_job_name():
    """Test extracting job name from pipeline name"""
    pipeline_name = 'yaetos__test_s_job_d_py__20230401T123456'
    job_name = DeployPySparkScriptOnAws.get_job_name(pipeline_name)
    assert job_name == 'test/job.py'

def test_temp_bucket_exists_success(deployer):
    """Test successful bucket existence check"""
    mock_s3 = Mock()
    mock_s3.meta.client.head_bucket.return_value = True
    
    deployer.temp_bucket_exists(mock_s3)
    mock_s3.meta.client.head_bucket.assert_called_once_with(Bucket='test-bucket')

def test_temp_bucket_exists_failure(deployer):
    """Test bucket not found error"""
    mock_s3 = Mock()
    error_response = {'Error': {'Code': '404'}}
    mock_s3.meta.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        error_response, 'HeadBucket')
    
    with pytest.raises(SystemExit):
        deployer.temp_bucket_exists(mock_s3)

@patch('yaetos.deploy.os.path.isfile')
def test_continue_post_git_check_no_prod(mock_isfile, deployer):
    """Test git check when not in prod mode"""
    deployer.app_args['mode'] = 'dev_EMR'
    assert deployer.continue_post_git_check() is True

@patch('boto3.session.Session')
def test_s3_ops(mock_session, deployer):
    """Test S3 operations sequence"""
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
    """Test creation of tar file with python scripts"""
    # Setup mocks
    mock_walk.return_value = [
        ('jobs', [], ['test_job.py']),
    ]
    mock_listdir.return_value = ['test_file.py']
    mock_tar = Mock()
    mock_tarfile.return_value = mock_tar
    
    # Create temp directory structure
    deployer.TMP = tmp_path / "tmp/files_to_ship/"
    deployer.TMP.mkdir(parents=True)
    
    deployer.tar_python_scripts()
    
    # Verify tar file was created
    assert mock_tar.add.called
    assert (deployer.TMP / "scripts.tar.gz").exists()

def test_get_spark_submit_args():
    """Test spark submit arguments generation"""
    app_file = 'test_job.py'
    app_args = {
        'py_job': 'jobs/test_job.py',
        'mode': 'dev_EMR',
        'job_param_file': 'conf/jobs_metadata.yml'
    }
    
    result = DeployPySparkScriptOnAws.get_spark_submit_args(app_file, app_args)
    
    assert '--py-files' in result
    assert 'scripts.zip' in result
    assert '--job_param_file' in result


# --- EMR Operations Tests ---

@patch('yaetos.deploy_emr.EMRer.run_direct')
def test_run_direct(mock_run_direct, deployer):
    """Test EMR direct run"""
    deployer.run_direct()
    mock_run_direct.assert_called_once()

def test_get_active_clusters(deployer):
    """Test getting active EMR clusters"""
    mock_client = Mock()
    mock_client.list_clusters.return_value = {
        'Clusters': [
            {'Id': 'j-123', 'Status': {'State': 'WAITING'}},
            {'Id': 'j-456', 'Status': {'State': 'RUNNING'}}
        ]
    }
    
    clusters = deployer.get_active_clusters(mock_client)
    assert len(clusters) == 2
    assert clusters[0]['Id'] == 'j-123'

def test_choose_cluster(deployer):
    """Test cluster selection"""
    clusters = [
        {'Id': 'j-123', 'Status': {'State': 'WAITING'}},
        {'Id': 'j-456', 'Status': {'State': 'RUNNING'}}
    ]
    
    # Test with specific cluster ID
    selected = deployer.choose_cluster(clusters, cluster_id='j-456')
    assert selected['Id'] == 'j-456'
    
    # Test without specific cluster ID (should take first available)
    selected = deployer.choose_cluster(clusters)
    assert selected['Id'] == 'j-123'

@patch('yaetos.deploy_emr.EMRer.start_spark_cluster')
def test_start_spark_cluster(mock_start_cluster, deployer):
    """Test EMR cluster startup"""
    mock_client = Mock()
    mock_start_cluster.return_value = 'j-789'
    
    cluster_id = deployer.start_spark_cluster(mock_client, 'emr-6.1.1')
    assert cluster_id == 'j-789'
    mock_start_cluster.assert_called_once()

# --- Kubernetes Deployment Tests ---

@patch('yaetos.deploy_k8s.Kuberneter.run_direct_k8s')
def test_run_direct_k8s(mock_run_k8s, deployer):
    """Test Kubernetes direct run"""
    deployer.run_direct_k8s()
    mock_run_k8s.assert_called_once()

def test_get_spark_submit_args_k8s():
    """Test Kubernetes spark-submit arguments generation"""
    app_file = 'test_job.py'
    app_args = {
        'py_job': 'jobs/test_job.py',
        'mode': 'dev_k8s',
        'k8s_namespace': 'spark',
        'k8s_service_account': 'spark-sa'
    }
    
    result = DeployPySparkScriptOnAws.get_spark_submit_args_k8s(app_file, app_args)
    assert '--master k8s://' in result
    assert '--conf spark.kubernetes.namespace=' in result

@patch('yaetos.deploy_k8s.Kuberneter.launch_spark_submit_k8s')
def test_launch_spark_submit_k8s(mock_launch, deployer):
    """Test Kubernetes spark-submit launch"""
    cmdline = '--master k8s:// --deploy-mode cluster'
    deployer.launch_spark_submit_k8s(cmdline)
    mock_launch.assert_called_once_with(deployer, cmdline)

# --- AWS Data Pipeline Tests ---

@patch('yaetos.deploy_aws_data_pipeline.AWS_Data_Pipeliner.run_aws_data_pipeline')
def test_run_aws_data_pipeline(mock_run_pipeline, deployer):
    """Test AWS Data Pipeline run"""
    deployer.run_aws_data_pipeline()
    mock_run_pipeline.assert_called_once()

def test_create_data_pipeline(deployer):
    """Test Data Pipeline creation"""
    mock_client = Mock()
    mock_client.create_pipeline.return_value = {'pipelineId': 'df-123'}
    
    pipe_id = deployer.create_data_pipeline(mock_client)
    assert pipe_id == 'df-123'
    mock_client.create_pipeline.assert_called_once()

def test_define_data_pipeline(deployer):
    """Test Data Pipeline definition"""
    mock_client = Mock()
    pipe_id = 'df-123'
    emr_instances = 2
    
    deployer.define_data_pipeline(mock_client, pipe_id, emr_instances)
    mock_client.put_pipeline_definition.assert_called_once()

def test_list_data_pipeline(deployer):
    """Test listing Data Pipelines"""
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
    """Test Airflow run"""
    deployer.run_aws_airflow()
    mock_run_airflow.assert_called_once()

def test_create_dags(deployer):
    """Test DAG creation"""
    with patch('builtins.open', create=True), \
         patch('yaetos.deploy_airflow.Airflower.create_dags') as mock_create:
        
        mock_create.return_value = ('local_dag.py', 'test_dag')
        fname_local, job_dag_name = deployer.create_dags()
        
        assert fname_local == 'local_dag.py'
        assert job_dag_name == 'test_dag'

def test_set_job_dag_name(deployer):
    """Test setting DAG name"""
    job_name = 'test_job'
    dag_name = deployer.set_job_dag_name(job_name)
    assert job_name in dag_name
    assert dag_name.startswith('yaetos_')

@patch('yaetos.deploy_airflow.Airflower.upload_dags')
def test_upload_dags(mock_upload, deployer):
    """Test DAG upload to S3"""
    mock_s3 = Mock()
    s3_dags = 's3://airflow-dags'
    job_dag_name = 'test_dag'
    fname_local = 'local_dag.py'
    
    deployer.upload_dags(mock_s3, s3_dags, job_dag_name, fname_local)
    mock_upload.assert_called_once()

# --- Integration Test Examples ---

@patch('boto3.session.Session')
def test_full_emr_deployment_flow(mock_session, deployer):
    """Test full EMR deployment flow"""
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
    """Test full AWS Data Pipeline deployment flow"""
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