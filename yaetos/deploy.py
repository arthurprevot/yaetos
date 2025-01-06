# encoding: utf-8
"""
Code running on client side to push code to AWS and execute it there.
Borrows from https://github.com/thomhopmans/themarketingtechnologist/tree/master/6_deploy_spark_cluster_on_aws
"""

# TODO:
# - move all print statements to logger
# - check why cluster setup is not exactly the same when using run_direct() or run_aws_data_pipeline()
# - check that bucket where output will be is available (to avoid loosing time before crashing)

import os
from datetime import datetime
import tarfile
import zipfile
import botocore
from pathlib import Path as Pt
from cloudpathlib import CloudPath as CPt
from pprint import pformat
from configparser import ConfigParser
from shutil import copyfile
import site
import yaetos.etl_utils as eu
from yaetos.git_utils import Git_Config_Manager
from yaetos.deploy_emr import EMRer
from yaetos.deploy_k8s import Kuberneter
from yaetos.deploy_aws_data_pipeline import AWS_Data_Pipeliner
from yaetos.deploy_airflow import Airflower
from yaetos.deploy_utils import terminate
from yaetos.logger import setup_logging
logger = setup_logging('Deploy')


class DeployPySparkScriptOnAws(object):
    """
    Programmatically deploy a local PySpark script on an AWS cluster
    """
    SCRIPTS = Pt('yaetos/scripts/')  # TODO: move to etl_utils.py
    TMP = Pt('tmp/files_to_ship/')

    def __init__(self, deploy_args, app_args):

        logger.info("etl deploy_args: \n{}".format(pformat(deploy_args)))
        logger.info("etl app_args: \n{}".format(pformat(app_args)))
        aws_setup = deploy_args['aws_setup']
        config = ConfigParser()
        assert os.path.isfile(deploy_args['aws_config_file'])
        config.read(deploy_args['aws_config_file'])

        self.app_args = app_args
        self.app_file = app_args['py_job']  # TODO: remove all refs to app_file to be consistent.
        self.aws_setup = aws_setup
        # From aws_config.cfg:
        self.ec2_key_name = config.get(aws_setup, 'ec2_key_name')
        self.s3_region = config.get(aws_setup, 's3_region')
        self.user = config.get(aws_setup, 'user')
        self.profile_name = config.get(aws_setup, 'profile_name')
        self.ec2_subnet_id = config.get(aws_setup, 'ec2_subnet_id')
        self.extra_security_gp = config.get(aws_setup, 'extra_security_gp', fallback=None)
        self.emr_ec2_role = config.get(aws_setup, 'emr_ec2_role', fallback='EMR_EC2_DefaultRole')
        self.emr_role = config.get(aws_setup, 'emr_role', fallback='EMR_DefaultRole')
        # From jobs_metadata.yml:
        self.emr_core_instances = int(app_args.get('emr_core_instances', 1))  # TODO: make this update EMR_Scheduled mode too.
        self.deploy_args = deploy_args
        self.ec2_instance_master = app_args.get('ec2_instance_master', 'm5.xlarge')  # 'm5.12xlarge', # used m3.2xlarge (8 vCPU, 30 Gib RAM), and earlier m3.xlarge (4 vCPU, 15 Gib RAM)
        self.ec2_instance_slaves = app_args.get('ec2_instance_slaves', 'm5.xlarge')
        self.emr_applications = app_args.get('emr_applications', [{'Name': 'Hadoop'}, {'Name': 'Spark'}])
        # Computed params:
        s3_logs = app_args.get('s3_logs', 's3://').replace('{{root_path}}', self.app_args.get('root_path', ''))
        self.s3_logs = CPt(s3_logs)
        self.s3_bucket_logs = self.s3_logs.bucket
        self.metadata_folder = 'pipelines_metadata'  # TODO remove
        self.pipeline_name = self.generate_pipeline_name(self.deploy_args['mode'], self.app_args['job_name'], self.user)  # format: some_job.some_user.20181204.153429
        self.job_log_path = self.get_job_log_path()  # format: yaetos/logs/some_job.some_user.20181204.153429
        self.job_log_path_with_bucket = '{}/{}'.format(self.s3_bucket_logs, self.job_log_path)   # format: bucket-tempo/yaetos/logs/some_job.some_user.20181204.153429
        self.package_path = self.job_log_path + '/code_package'   # format: yaetos/logs/some_job.some_user.20181204.153429/package
        self.package_path_with_bucket = self.job_log_path_with_bucket + '/code_package'   # format: bucket-tempo/yaetos/logs/some_job.some_user.20181204.153429/package

        spark_version = self.deploy_args.get('spark_version', '3.0')
        if spark_version == '2.4':
            self.emr_version = "emr-5.26.0"
            # used "emr-5.26.0" successfully for a while. emr-6.0.0 is latest as of june 2020, first with python3 by default but not supported by AWS Data Pipeline, emr-5.26.0 is latest as of aug 2019 # Was "emr-5.8.0", which was compatible with m3.2xlarge.
        elif spark_version == '3.0':
            self.emr_version = "emr-6.1.1"  # latest compatible with AWS Data Piupeline, # see latest supported emr version by AWS Data Pipeline at https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-emrcluster.html
        elif spark_version == '3.4':
            self.emr_version = "emr-6.15.0"  # not compatible with "AWS Data Pipeline" but compatible with Airflow. Inc Python 3.7.16
        elif spark_version == '3.5':
            self.emr_version = "emr-7.0.0"  # not compatible with "AWS Data Pipeline" but compatible with Airflow. Inc Python 3.9.16

        if self.deploy_args.get('monitor_git', False):  # TODO: centralize monitor_git
            try:
                self.git_yml = Git_Config_Manager().get_config_from_git(eu.LOCAL_FRAMEWORK_FOLDER)
                Git_Config_Manager().save_yaml(self.git_yml)
            except Exception as e:  # TODO: get specific exception
                self.git_yml = None
                logger.debug("Didn't save yml file with git info, expected if running for the pip installed library. message '{}'.".format(e))

    def run(self):
        if self.continue_post_git_check() is False:
            return False

        self.session = eu.get_aws_setup(self.deploy_args)
        if not self.app_args.get('skip_aws_check', False):
            eu.test_aws_connection(self.session)

        if self.deploy_args['deploy'] == 'EMR':
            self.run_direct()
        elif self.deploy_args['deploy'] == 'k8s':
            self.run_direct_k8s()
        elif self.deploy_args['deploy'] in ('EMR_Scheduled', 'EMR_DataPipeTest'):
            self.run_aws_data_pipeline()
        elif self.deploy_args['deploy'] in ('airflow', 'airflow_k8s'):
            self.run_aws_airflow()
        elif self.deploy_args['deploy'] in ('code'):
            self.run_push_code()
        else:
            raise Exception("Shouldn't get here.")

    def continue_post_git_check(self):
        if 'prod_EMR' not in self.app_args['mode'].split(','):
            logger.debug('Not pushing as "prod_EMR", so git check ignored')
            return True
        elif self.git_yml is None:
            logger.debug('Code not git controled: git check ignored')
            return True

        git_yml = {key: value for key, value in self.git_yml.items() if key in ('is_dirty_yaetos', 'is_dirty_current', 'branch_current', 'branch_yaetos')}
        if self.git_yml['is_dirty_current'] or self.git_yml['is_dirty_yaetos']:
            logger.info('Some changes to your git controled files are not committed to git: {}'.format(git_yml))
            answer = input('Are you sure you want to deploy it ? [y/n] ')
            if answer == 'y':
                logger.info('Ok, continuing deployment')
                return True
            elif answer == 'n':
                logger.info('Ok, cancelling deployment')
                return False
            else:
                logger.info('Answer not understood, it should be "y" or "n", cancelling deployment')
                return False
        else:
            logger.info('Git controled files are clean, continuing with push to prod. Git setup: {}'.format(git_yml))
            return True

    def run_push_code(self):
        logger.info("Pushing code only")
        self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

    def run_direct(self):
        # TODO: integrate deploy_emr properly
        return EMRer.run_direct(self)

    def run_direct_k8s(self):
        # TODO: integrate deploy_k8s properly
        return Kuberneter.run_direct_k8s(self)

    @staticmethod
    def get_spark_submit_args_k8s(app_file, app_args):
        # TODO: integrate deploy_k8s properly
        spark_submit = Kuberneter.get_spark_submit_args_k8s(app_file, app_args)
        return spark_submit

    def launch_spark_submit_k8s(self, cmdline):
        # TODO: integrate deploy_k8s properly
        return Kuberneter.launch_spark_submit_k8s(self, cmdline)

    def s3_ops(self, session):
        s3 = session.resource('s3')
        self.temp_bucket_exists(s3)
        self.local_file_ops()
        self.move_bash_to_local_temp()
        self.upload_temp_files(s3)
        return s3

    def local_file_ops(self):
        self.tar_python_scripts()
        self.convert_tar_to_zip()

    def get_active_clusters(self, c):
        # TODO: integrate deploy_emr properly
        return EMRer.get_active_clusters(self, c)

    def choose_cluster(self, clusters, cluster_id=None):
        # TODO: integrate deploy_emr properly
        return EMRer.choose_cluster(self, clusters, cluster_id=None)

    @staticmethod
    def generate_pipeline_name(mode, job_name, user):
        """Opposite of get_job_name()"""
        pname = job_name.replace('.', '_d_').replace('/', '_s_')
        now = datetime.now().strftime("%Y%m%dT%H%M%S")
        name = f"yaetos__{pname}__{now}"
        logger.info('Pipeline Name "{}":'.format(name))
        return name

    @staticmethod
    def get_job_name(pipeline_name):
        """Extracts the original job name from a pipeline name by reversing the transformations done in generate_pipeline_name()."""
        return pipeline_name.split('__')[2].replace('_d_', '.').replace('_s_', '/') if '__' in pipeline_name else None
        # if not pipeline_name or '__' not in pipeline_name:
        #     return None
            
        # parts = pipeline_name.split('__')
        # if len(parts) != 3:
        #     return None
            
        # transformed_name = parts[1]
        # job_name = transformed_name.replace('_d_', '.').replace('_s_', '/')
        # return job_name

    def get_job_log_path(self):
        if self.deploy_args.get('mode') and 'prod_EMR' in self.deploy_args.get('mode').split(','):  # TODO: check if should be replaced by app_args
            return '{}/jobs_code/production'.format(self.metadata_folder)
        else:
            return '{}/jobs_code/{}'.format(self.metadata_folder, self.pipeline_name)

    def temp_bucket_exists(self, s3):
        """
        Check if the bucket we are going to use for temporary files exists.
        :param s3:
        :return:
        """
        try:
            s3.meta.client.head_bucket(Bucket=self.s3_bucket_logs)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                terminate(f"Bucket for logs does not exist (check param 's3_logs'): {self.s3_bucket_logs} {e.response}")
            terminate(f"Error while connecting to Bucket for logs (check param 's3_logs'): {self.s3_bucket_logs} {e.response}")
        logger.debug("S3 bucket for temporary files exists: " + self.s3_bucket_logs)

    def tar_python_scripts(self):
        package = self.get_package_path()
        logger.info(f"Package (tar.gz) to be created from files in '{package}', to be put in {self.TMP}")
        output_path = self.TMP / "scripts.tar.gz"

        # Create tar.gz file
        t_file = tarfile.open(output_path, 'w:gz')

        # Add config files
        if self.app_args['job_param_file']:
            t_file.add(self.app_args['job_param_file'], arcname=eu.JOBS_METADATA_FILE)

        git_yml = Pt('conf/git_config.yml')
        if os.path.isfile(git_yml):
            t_file.add(git_yml, arcname=git_yml)

        # ./yaetos files
        # TODO: check a way to deploy the yaetos code locally for testing.
        files = os.listdir(package / 'yaetos/')
        for f in files:
            t_file.add(package / 'yaetos/' / f, arcname='yaetos/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        # ./libs files
        # TODO: get better way to walk down tree (reuse walk from below)
        files = os.listdir(package / 'yaetos/libs/')
        for f in files:
            t_file.add(package / 'yaetos/libs/' / f, arcname='yaetos/libs/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        files = os.listdir(package / 'yaetos/libs/analysis_toolkit/')
        for f in files:
            t_file.add(package / 'yaetos/libs/analysis_toolkit/' / f, arcname='yaetos/libs/analysis_toolkit/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        files = os.listdir(package / 'yaetos/libs/python_db_connectors/')
        for f in files:
            t_file.add(package / 'yaetos/libs/python_db_connectors/' / f, arcname='yaetos/libs/python_db_connectors/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        # ./jobs files and folders
        # TODO: extract code below in external function.
        files = []
        folder_to_skip = ('bg-jobs')  # bg-jobs contains intermediate jars from scala compilation process.
        for (dirpath, dirnames, filenames) in os.walk(self.app_args['jobs_folder']):
            for file in filenames:
                if folder_to_skip in dirnames:
                    dirnames.remove(folder_to_skip)
                    continue

                if file.endswith(".py") or file.endswith(".sql") or file.endswith(".jar"):
                    path = os.path.join(dirpath, file)
                    dir_tar = dirpath[len(self.app_args['jobs_folder']):]
                    path_tar = os.path.join(eu.JOB_FOLDER, dir_tar, file)
                    files.append((path, path_tar))
        for f, f_arc in files:
            t_file.add(f, arcname=f_arc)

        # List all files in tar.gz
        for f in t_file.getnames():
            logger.debug("Added %s to tar-file" % f)
        t_file.close()
        logger.debug("Added all spark app files to {}".format(output_path))
        self.output_path = output_path

    def convert_tar_to_zip(self):
        tar_gz_path = self.output_path
        zip_path = self.TMP / "scripts.zip"
        with tarfile.open(tar_gz_path, 'r:gz') as tar:
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for member in tar.getmembers():
                    if member.isfile():
                        fileobj = tar.extractfile(member)
                        file_data = fileobj.read()
                        zipf.writestr(member.name, file_data)
        logger.info(f"Converted '{tar_gz_path}' to '{zip_path}'.")

    def move_bash_to_local_temp(self):
        """Moving file from local repo to local tmp folder for later upload to S3."""
        # Copy from lib or repo
        package = self.get_package_path()
        for item in ['setup_master.sh',
                     'setup_master_alt.sh',
                     'requirements_base.txt',
                     'requirements_base_alt.txt',
                     'setup_nodes.sh',
                     'setup_nodes_alt.sh',
                     'terminate_idle_cluster.sh']:
            source = package / self.SCRIPTS / item
            destination = self.TMP / item
            convert_to_linux_eol_if_needed(source)
            copyfile(source, destination)

        # Copy extra file from local folders
        item = 'requirements_extra.txt'
        source = Pt(f'conf/{item}')
        destination = self.TMP / item
        convert_to_linux_eol_if_needed(source)
        copyfile(source, destination)

        logger.debug("Added all EMR setup files to {}".format(self.TMP))

    def get_package_path(self):
        """
        Getting the package path depending on whether the core code is from lib (through pip install) or from local repo (for faster dev iterations).
        """
        if self.app_args['code_source'] == 'lib':
            bases = site.getsitepackages()
            if len(bases) > 1:
                logger.info("There is more than one source of code to ship to EMR '{}'. Will continue with the first one.".format(bases))
            base = Pt(bases[0])
        elif self.app_args['code_source'] == 'repo':
            base = Pt(eu.LOCAL_FRAMEWORK_FOLDER)
        elif self.app_args['code_source'] == 'dir':
            base = Pt(self.app_args['code_source_path'])
        logger.info("Source of yaetos code to be shipped: {}".format(base / 'yaetos/'))
        # TODO: move code_source and code_source_path to deploy_args, involves adding it to DEPLOY_ARGS_LIST
        return base

    def upload_temp_files(self, s3):
        """
        Move the PySpark + bash scripts to the S3 bucket we use to store temporary files
        """
        setup_master = 'setup_master_alt.sh' if self.deploy_args.get('spark_version', '2.4') == '2.4' else 'setup_master.sh'
        setup_nodes = 'setup_nodes_alt.sh' if self.deploy_args.get('spark_version', '2.4') == '2.4' else 'setup_nodes.sh'
        requirements = 'requirements_base_alt.txt' if self.deploy_args.get('spark_version', '2.4') == '2.4' else 'requirements_base.txt'

        # Looping through all 4 steps below doesn't work (Fails silently) so done 1 by 1.
        s3.Object(self.s3_bucket_logs, self.package_path + '/setup_master.sh')\
          .put(Body=open(str(self.TMP / setup_master), 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/setup_nodes.sh')\
          .put(Body=open(str(self.TMP / setup_nodes), 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/requirements.txt')\
          .put(Body=open(str(self.TMP / requirements), 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/requirements_extra.txt')\
          .put(Body=open(str(self.TMP / 'requirements_extra.txt'), 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/terminate_idle_cluster.sh')\
          .put(Body=open(str(self.TMP / 'terminate_idle_cluster.sh'), 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/scripts.tar.gz')\
          .put(Body=open(str(self.TMP / 'scripts.tar.gz'), 'rb'), ContentType='application/x-tar')
        logger.info(f"Uploaded job files (scripts.tar.gz, {setup_master}, {setup_nodes}, {requirements}, requirements_extra.txt, terminate_idle_cluster.sh) to bucket path '{self.s3_bucket_logs}/{self.package_path}'")
        return True

    def remove_temp_files(self, s3):
        """
        Remove Spark files from temporary bucket
        :param s3:
        :return:
        """
        bucket = s3.Bucket(self.s3_bucket_logs)
        for key in bucket.objects.all():
            if key.key.startswith(self.pipeline_name) is True:
                key.delete()
                logger.info("Removed '{}' from bucket for temporary files".format(key.key))

    def start_spark_cluster(self, c, emr_version):
        # TODO: integrate deploy_emr properly
        return EMRer.start_spark_cluster(self, c, emr_version)

    def describe_status_until_terminated(self, c):
        # TODO: integrate deploy_emr properly
        return EMRer.describe_status_until_terminated(self, c)

    def describe_status(self, c):
        # TODO: integrate deploy_emr properly
        return EMRer.describe_status(self, c)

    def step_run_setup_scripts(self, c):
        # TODO: integrate deploy_emr properly
        return EMRer.step_run_setup_scripts(self, c)

    def step_spark_submit(self, c, app_file, app_args):
        # TODO: integrate deploy_emr properly
        return EMRer.step_spark_submit(self, c, app_file, app_args)

    @staticmethod
    def get_spark_submit_args(app_file, app_args):
        """ app_file is launcher, might be py_job too, but may also be separate from py_job (ex python launcher.py --job_name=some_job_with_py_job)."""

        if app_args.get('py_job'):
            overridable_args = {
                'spark_submit_args': '--verbose',
                'spark_submit_keys': 'py-files',
                'spark_app_args': '',
                'spark_app_keys': 'mode--deploy--storage'}
        else:  # for jar_job
            overridable_args = {
                'spark_submit_args': '--verbose',
                'spark_submit_keys': '',
                'spark_app_args': '',
                'spark_app_keys': ''}

        overridable_args.update(app_args)
        args = overridable_args.copy()

        # set py_job
        if app_args.get('launcher_file') and app_args.get('py_job'):
            py_job = eu.CLUSTER_APP_FOLDER + app_args.get('launcher_file')
        elif isinstance(app_file, str) and app_file.endswith('.py'):  # TODO: check values app_file can take
            py_job = eu.CLUSTER_APP_FOLDER + app_file
        else:
            py_job = None

        # set jar_job
        if (app_args.get('launcher_file') or isinstance(app_file, str)) and app_args.get('jar_job'):  # TODO: check to enforce app_args.get('launcher_file')
            jar_job = eu.CLUSTER_APP_FOLDER + app_args.get('jar_job')
        else:
            jar_job = None
        # TODO: simplify business of getting application code (2 blocks up) upstream, in etl_utils.py

        unoverridable_args = {
            'py-files': f"{eu.CLUSTER_APP_FOLDER}scripts.zip" if py_job else None,
            'py_job': py_job,
            'mode': app_args.get('default_aws_modes', 'dev_EMR') if app_args.get('mode') and 'dev_local' in app_args['mode'].split(',') else app_args.get('mode'),
            'deploy': 'none',
            'storage': 's3',
            'jar_job': jar_job}
        args.update(unoverridable_args)

        if app_args.get('load_connectors', '') == 'all':
            args['packages'] = app_args.get('spark_packages') or ','.join(eu.PACKAGES_EMR_SPARK_3),  # may not be used in spark-submit depending on 'load_connectors' para above.
            args['jars'] = app_args.get('spark_jars') or eu.JARS,  # may not be used in spark-submit depending on 'load_connectors' para above.
            args['spark_submit_keys'] += '--packages--jars'

        if app_args.get('dependencies'):
            args['spark_app_args'] += ' --dependencies'

        if app_args.get('chain_dependencies'):
            args['spark_app_args'] += ' --chain_dependencies'

        if app_args.get('job_param_file') and app_args.get('py_job'):
            args['job_param_file'] = eu.CLUSTER_APP_FOLDER + app_args['job_param_file']
            args['spark_app_keys'] += '--job_param_file'

        if app_args.get('sql_file'):
            args['sql_file'] = eu.CLUSTER_APP_FOLDER + app_args['sql_file']
            args['spark_app_keys'] += '--sql_file'

        if app_args.get('job_name') and app_args.get('py_job'):
            args['job_name'] = app_args['job_name']
            args['spark_app_keys'] += '--job_name'

        # TODO: implement better way to handle params, less case by case, to only deal with overloaded params
        jargs = eu.Job_Args_Parser(defaults_args={}, yml_args={}, job_args=args, cmd_args={}, build_yml_args=False, loaded_inputs={})
        return eu.Runner.create_spark_submit(jargs)

    def run_aws_data_pipeline(self):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.run_aws_data_pipeline(self)

    def create_data_pipeline(self, client):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.create_data_pipeline(self, client)

    def define_data_pipeline(self, client, pipe_id, emr_core_instances):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.define_data_pipeline(self, client, pipe_id, emr_core_instances)

    def activate_data_pipeline(self, client, pipe_id, parameterValues):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.activate_data_pipeline(self, client, pipe_id, parameterValues)

    def list_data_pipeline(self, client):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.list_data_pipeline(self, client)

    def deactivate_similar_pipelines(self, client, pipeline_id):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.deactivate_similar_pipelines(self, client, pipeline_id)

    def update_params(self, parameterValues):
        # TODO: integrate deploy_aws_data_pipeline properly
        return AWS_Data_Pipeliner.update_params(self, parameterValues)

    def run_aws_airflow(self):
        # TODO: integrate deploy_airflow properly
        return Airflower.run_aws_airflow(self)

    def create_dags(self):
        # TODO: integrate deploy_airflow properly
        fname_local, job_dag_name = Airflower.create_dags(self)
        return fname_local, job_dag_name

    def set_job_dag_name(self, jobname):
        # TODO: integrate deploy_airflow properly
        return Airflower.set_job_dag_name(self, jobname)

    @staticmethod
    def upload_dags(s3, s3_dags, job_dag_name, fname_local):
        # TODO: integrate deploy_airflow properly
        return Airflower.upload_dags(s3, s3_dags, job_dag_name, fname_local)

    def push_secrets(self, creds_or_file):
        client = self.session.client('secretsmanager')

        file = open(creds_or_file, "r")
        content = file.read()
        file.close()

        try:
            response = client.create_secret(
                Name=eu.AWS_SECRET_ID,
                SecretString=content,
            )
            logger.debug('create_secret response: ' + str(response))
            logger.info('Created aws secret, from {}, under secret_id:{}'.format(creds_or_file, eu.AWS_SECRET_ID))
        except client.exceptions.ResourceExistsException:
            response = client.put_secret_value(
                SecretId=eu.AWS_SECRET_ID,
                SecretString=content,
            )
            logger.debug('put_secret_value response: ' + str(response))
            logger.info('Updated aws secret, from {}, under secret_id:{}'.format(creds_or_file, eu.AWS_SECRET_ID))

    def delete_secrets(self):
        """ To be used manually for now to free AWS resources. """
        client = self.session.client('secretsmanager')

        response = client.delete_secret(
            SecretId=eu.AWS_SECRET_ID,
            # RecoveryWindowInDays=123,
            ForceDeleteWithoutRecovery=True
        )
        logger.debug('delete_secret response: ' + str(response))
        logger.info('Deleted aws secret, secret_id:' + eu.AWS_SECRET_ID)
        logger.info('delete_secret response: {}'.format(response))


def deploy_all_scheduled():
    # Experimental ! Has lead to errors like: /usr/bin/python3: can't open file '/home/hadoop/app/jobs/frontroom/hotel_staff_usage_job.py': [Errno 2] No such file or directory
    # pb I don't get when deploying normally, from job files.
    # TODO: also need to remove "dependency" run for the ones with no dependencies.
    def get_yml(args):
        meta_file = args.get('job_param_file', 'repo')
        if meta_file == 'repo':
            meta_file = eu.CLUSTER_APP_FOLDER + eu.JOBS_METADATA_FILE if args['storage'] == 's3' else eu.JOBS_METADATA_LOCAL_FILE
        yml = eu.Job_Args_Parser.load_meta(meta_file)
        logger.info('Loaded job param file: ' + meta_file)
        return yml

    def get_bool(prompt):
        while True:
            try:
                return {"": True, "y": True, "n": False}[input(prompt).lower()]
            except KeyError:
                logger.info("Invalid input please enter y or n!")

    def validate_job(job):
        return get_bool('Want to schedule "{}" [Y/n]? '.format(job))

    # TODO: reuse etl_utils.py Commandliner/set_commandline_args() to have cleaner interface and proper default values.
    deploy_args = {'leave_on': False,
                   'aws_config_file': eu.AWS_CONFIG_FILE,  # TODO: make set-able
                   'aws_setup': 'dev'}
    app_args = {'deploy': 'EMR_Scheduled',
                'job_param_file': 'conf/jobs_metadata.yml',  # TODO: make set-able. Set to external repo for testing.
                'chain_dependencies': False,
                'dependencies': True,
                'storage': 'local',
                'jobs_folder': eu.JOB_FOLDER,  # TODO: make set-able
                'connection_file': eu.CONNECTION_FILE,  # TODO: make set-able
                }

    yml = get_yml(app_args)
    pipelines = yml.keys()
    for pipeline in pipelines:
        jargs = eu.Job_Args_Parser(app_args)
        jargs.set_job_params(job_name=pipeline)  # broken TODO: fix.
        if not jargs.frequency:
            continue

        run = validate_job(pipeline)
        if not run:
            continue

        DeployPySparkScriptOnAws(deploy_args, app_args).run()


def convert_to_linux_eol_if_needed(fname):
    """ Function needed when running from windows to avoid error registering AWS EMR step : 'No such file or directory'"""
    if os.name == 'nt':
        from yaetos.windows_utils import convert_to_linux_eol  # loaded here to remove dependency for non windows users.
        convert_to_linux_eol(fname, fname)


# def terminate(error_message=None):
#     """
#     Method to exit the Python script. It will log the given message and then exit().
#     :param error_message:
#     """
#     if error_message:
#         logger.error(error_message)
#     logger.critical('The script is now terminating')
#     exit()


def deploy_standalone(job_args_update={}):
    # TODO: refactor below to use 'deploy' arg to trigger all deploy features, instead of new 'deploy_option' set below.
    job_args = {
        # --- regular job params ---
        'job_param_file': None,
        'mode': 'dev_EMR',  # TODO: make independent from dev_EMR
        'output': {'path': 'n_a', 'type': 'csv'},
        'job_name': 'n_a',
        # --- params specific to running this file directly, can be overriden by command line ---
        'deploy_option': 'deploy_code_only',
    }
    job_args.update(job_args_update)

    parser, defaults_args = eu.Commandliner.define_commandline_args()
    cmd_args = eu.Commandliner.set_commandline_args(parser)
    jargs = eu.Job_Args_Parser(defaults_args=defaults_args, yml_args=None, job_args=job_args, cmd_args=cmd_args, loaded_inputs={})
    deploy_args = jargs.get_deploy_args()
    app_args = jargs.get_app_args()

    if jargs.deploy_option == 'deploy_job':  # can be used to push random code to cluster
        # TODO: fails to create a new cluster but works to add a step to an existing cluster.
        DeployPySparkScriptOnAws(deploy_args, app_args).run()

    elif jargs.deploy_option == 'deploy_code_only':
        deploy_args['deploy'] = 'code'
        DeployPySparkScriptOnAws(deploy_args, app_args).run()

    elif jargs.deploy_option == 'show_list_pipelines':
        deployer = DeployPySparkScriptOnAws(deploy_args, app_args)
        client = deployer.session.client('datapipeline')
        pipelines = deployer.list_data_pipeline(client)
        logger.info('#--- pipelines: ', pipelines)

    elif jargs.deploy_option == 'deploy_all_jobs':
        deploy_all_scheduled()  # TODO: needs more testing.

    elif jargs.deploy_option == 'package_code_locally_only':  # only for debuging
        deployer = DeployPySparkScriptOnAws(deploy_args, app_args)  # TODO: should remove need for some of these inputs as they are not required by tar_python_scripts()
        pipelines = deployer.tar_python_scripts()
        logger.info('#--- Finished packaging ---')
    return True


if __name__ == "__main__":
    deploy_standalone()
