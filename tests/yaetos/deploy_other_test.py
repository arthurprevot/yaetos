import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import boto3
import botocore
from yaetos.deploy import DeployPySparkScriptOnAws
from yaetos.etl_utils import Job_Args_Parser

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

if __name__ == '__main__':
    pytest.main([__file__])