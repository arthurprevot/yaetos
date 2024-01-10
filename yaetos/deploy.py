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
import time
import tarfile
import boto3
import botocore
import uuid
import json
from pathlib import Path as Pt
from cloudpathlib import CloudPath as CPt
from pprint import pformat
from configparser import ConfigParser
from shutil import copyfile
import site
import yaetos.etl_utils as eu
from yaetos.git_utils import Git_Config_Manager
from yaetos.logger import setup_logging
from yaetos.airflow_template import get_template
logger = setup_logging('Deploy')


class DeployPySparkScriptOnAws(object):
    """
    Programmatically deploy a local PySpark script on an AWS cluster
    """
    SCRIPTS = Pt('yaetos/scripts/')  # TODO: move to etl_utils.py
    TMP = Pt('tmp/files_to_ship/')
    DAGS = Pt('tmp/files_to_ship/dags')

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
        self.ec2_key_name = config.get(aws_setup, 'ec2_key_name')
        self.s3_region = config.get(aws_setup, 's3_region')
        self.user = config.get(aws_setup, 'user')
        self.profile_name = config.get(aws_setup, 'profile_name')
        self.ec2_subnet_id = config.get(aws_setup, 'ec2_subnet_id')
        self.extra_security_gp = config.get(aws_setup, 'extra_security_gp')
        self.emr_core_instances = int(app_args.get('emr_core_instances', 1))  # TODO: make this update EMR_Scheduled mode too.
        self.deploy_args = deploy_args
        self.ec2_instance_master = app_args.get('ec2_instance_master', 'm5.xlarge')  # 'm5.12xlarge', # used m3.2xlarge (8 vCPU, 30 Gib RAM), and earlier m3.xlarge (4 vCPU, 15 Gib RAM)
        self.ec2_instance_slaves = app_args.get('ec2_instance_slaves', 'm5.xlarge')
        # Paths
        self.s3_logs = CPt(app_args.get('s3_logs', 's3://').replace('{root_path}', self.app_args.get('root_path', '')))
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
            # used "emr-5.26.0" successfully for a bit. emr-6.0.0 is latest as of june 2020, first with python3 by default but not supported by AWS Data Pipeline, emr-5.26.0 is latest as of aug 2019 # Was "emr-5.8.0", which was compatible with m3.2xlarge.
            # TODO: check switching to EMR 5.28 which has improvement to EMR runtime for spark.
        elif spark_version == '3.0':
            self.emr_version = "emr-6.1.1"
            # latest is "emr-6.3.0" but latest compatible with AWS Data Piupeline is "emr-6.1.0".
            # see latest supported emr version by AWS Data Pipeline at https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-emrcluster.html
        elif spark_version == '3.4':
            self.emr_version = "emr-6.15.0"  # not compatible with "AWS Data Pipeline" but should be with Airflow. Inc Python 3.7.16
        elif spark_version == '3.5':
            self.emr_version = "emr-7.0.0"  # not compatible with "AWS Data Pipeline" but should be with Airflow. Inc Python 3.7.16

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

        self.session = boto3.Session(profile_name=self.profile_name)  # aka AWS IAM profile
        if self.deploy_args['deploy'] == 'EMR':
            self.run_direct()
        elif self.deploy_args['deploy'] in ('EMR_Scheduled', 'EMR_DataPipeTest'):
            self.run_aws_data_pipeline()
        elif self.deploy_args['deploy'] in ('airflow'):
            self.run_aws_airflow()
        elif self.deploy_args['deploy'] in ('code'):
            self.run_push_code()
        else:
            raise Exception("Shouldn't get here.")

    def continue_post_git_check(self):
        if self.app_args['mode'] != 'prod_EMR':
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
        """Useful to run job on cluster without bothering with aws data pipeline. Also useful to add steps to existing cluster."""
        self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        # EMR ops
        c = self.session.client('emr')
        clusters = self.get_active_clusters(c)
        cluster = self.choose_cluster(clusters)
        new_cluster = cluster['id'] is None
        if new_cluster:
            logger.info("Starting new cluster")
            self.start_spark_cluster(c, self.emr_version)
            logger.info("cluster name: %s, and id: %s" % (self.pipeline_name, self.cluster_id))
            self.step_run_setup_scripts(c)
        else:
            logger.info("Reusing existing cluster, name: %s, and id: %s" % (cluster['name'], cluster['id']))
            self.cluster_id = cluster['id']
            self.step_run_setup_scripts(c)

        # Run job
        self.step_spark_submit(c, self.app_file, self.app_args)

        # Clean
        if new_cluster and not self.deploy_args.get('leave_on') and self.app_args.get('clean_post_run'):  # TODO: add clean_post_run in input options.
            logger.info("New cluster setup to be deleted after job finishes.")
            self.describe_status_until_terminated(c)
            s3 = self.session.resource('s3')
            self.remove_temp_files(s3)  # TODO: remove tmp files for existing clusters too but only tmp files for the job

    def s3_ops(self, session):
        s3 = session.resource('s3')
        self.temp_bucket_exists(s3)
        self.tar_python_scripts()
        self.move_bash_to_local_temp()
        self.upload_temp_files(s3)
        return s3

    def get_active_clusters(self, c):
        response = c.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'],
        )
        clusters = [(ii + 1, item['Id'], item['Name']) for ii, item in enumerate(response['Clusters'])]
        return clusters

    def choose_cluster(self, clusters, cluster_id=None):
        if len(clusters) == 0:
            logger.info('No cluster found, will create a new one')
            return {'id': None,
                    'name': None}

        if cluster_id is not None:
            logger.info('Cluster_id set by user to {}'.format(cluster_id))
            return {'id': cluster_id,
                    'name': None}

        clusters.append((len(clusters) + 1, None, 'Create a new cluster'))
        print('Clusters found for AWS account "%s":' % (self.aws_setup))
        print('\n'.join(['[%s] %s' % (item[0], item[2]) for item in clusters]))
        answer = input('Your choice ? ')
        return {'id': clusters[int(answer) - 1][1],
                'name': clusters[int(answer) - 1][2]}

    @staticmethod
    def generate_pipeline_name(mode, job_name, user):
        """Opposite of get_job_name()"""
        mode_label = {'dev_EMR': 'dev', 'prod_EMR': 'prod'}[mode]
        pname = job_name.replace('.', '_d_').replace('/', '_s_')
        now = datetime.now().strftime("%Y%m%dT%H%M%S")
        name = f"yaetos__{mode_label}__{pname}__{now}"
        logger.info('Pipeline Name "{}":'.format(name))
        return name

    @staticmethod
    def get_job_name(pipeline_name):
        """Opposite of generate_pipeline_name()"""
        return pipeline_name.split('__')[2].replace('_d_', '.').replace('_s_', '/') if '__' in pipeline_name else None

    def get_job_log_path(self):
        if self.deploy_args.get('mode') == 'prod_EMR':
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
                terminate(f"Bucket for temporary files does not exist: {self.s3_bucket_logs} {e.response}")
            terminate(f"Error while connecting to temporary Bucket: {self.s3_bucket_logs} {e.response}")
        logger.debug("S3 bucket for temporary files exists: " + self.s3_bucket_logs)

    def tar_python_scripts(self):
        package = self.get_package_path()
        logger.info(f"Package (.tar.gz) to be created from files in '{package}', to be put in {self.TMP}, and pushed to S3")
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
        for (dirpath, dirnames, filenames) in os.walk(self.app_args['jobs_folder']):
            for file in filenames:
                if file.endswith(".py") or file.endswith(".sql"):
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
        logger.debug("Source of yaetos code to be shipped: {}".format(base / 'yaetos/'))
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
        """
        :param c: EMR client
        :return:
        """
        instance_groups = [{
            'Name': 'EmrMaster',
            'InstanceRole': 'MASTER',
            'InstanceType': self.ec2_instance_master,
            'InstanceCount': 1,
        }]
        if self.emr_core_instances != 0:
            instance_groups += [{
                'Name': 'EmrCore',
                'InstanceRole': 'CORE',
                'InstanceType': self.ec2_instance_slaves,
                'InstanceCount': self.emr_core_instances,
            }]

        response = c.run_job_flow(
            Name=self.pipeline_name,
            LogUri="s3://{}/{}/manual_run_logs/".format(self.s3_bucket_logs, self.metadata_folder),
            ReleaseLabel=emr_version,
            Instances={
                'InstanceGroups': instance_groups,
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': self.deploy_args.get('leave_on', False),
                'Ec2SubnetId': self.ec2_subnet_id,
                # 'AdditionalMasterSecurityGroups': self.extra_security_gp,  # TODO : make optional in future. "[self.extra_security_gp] if self.extra_security_gp else []" doesn't work.
            },
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            Configurations=[
                {  # Section to force python3 since emr-5.x uses python2 by default.
                    "Classification": "spark-env",
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}
                    }]
                },
                # { # Section to add jars (redshift...), not used for now, since passed in spark-submit args.
                # "Classification": "spark-defaults",
                # "Properties": { "spark.jars": ["/home/hadoop/redshift_tbd.jar"], "spark.driver.memory": "40G", "maximizeResourceAllocation": "true"},
                # }
            ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            BootstrapActions=[{
                'Name': 'setup_nodes',
                'ScriptBootstrapAction': {
                    'Path': 's3n://{}/setup_nodes.sh'.format(self.package_path_with_bucket),
                    'Args': []
                }
            }],
        )
        # Process response to determine if Spark cluster was started, and if so, the JobFlowId of the cluster
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            self.cluster_id = response['JobFlowId']
        else:
            terminate("Could not create EMR cluster (status code {})".format(response_code))

        logger.info("Created Spark EMR cluster ({}) with cluster_id {}".format(emr_version, self.cluster_id))

    def describe_status_until_terminated(self, c):
        """
        :param c:
        :return:
        """
        logger.info('Waiting for job to finish on cluster')
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.cluster_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
                logger.info('Job is finished')
            logger.info('Cluster state:' + state)
            time.sleep(30)  # Prevent ThrottlingException by limiting number of requests

    def step_run_setup_scripts(self, c):
        """
        :param c:
        :return:
        """
        response = c.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[{
                'Name': 'Run Setup',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': f's3://{self.s3_region}.elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': [
                        "s3://{}/setup_master.sh".format(self.package_path_with_bucket),
                        "s3://{}".format(self.package_path_with_bucket),
                    ]
                }
            }]
        )
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            logger.debug(f"Added step 'run setup', using s3://{self.package_path_with_bucket}/setup_master.sh")
        else:
            raise Exception("Step couldn't be added")
        time.sleep(1)  # Prevent ThrottlingException

    def step_spark_submit(self, c, app_file, app_args):
        """
        :param c:
        :return:
        """
        cmd_runner_args = self.get_spark_submit_args(app_file, app_args)

        response = c.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[{
                'Name': 'Spark Application',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': cmd_runner_args
                }
            }]
        )
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            logger.info("Added step 'spark-submit' with command line '{}'".format(' '.join(cmd_runner_args)))
        else:
            raise Exception("Step couldn't be added")
        time.sleep(1)  # Prevent ThrottlingException

    def get_spark_submit_args(self, app_file, app_args):

        emr_mode = 'dev_EMR' if app_args['mode'] == 'dev_local' else app_args['mode']
        launcher_file = app_args.get('launcher_file') or app_file

        spark_submit_args = [
            "spark-submit",
            "--verbose",
            "--py-files={}scripts.zip".format(eu.CLUSTER_APP_FOLDER),
        ]
        if app_args.get('load_connectors', '') == 'all':
            package = eu.PACKAGES_EMR if self.deploy_args.get('spark_version', '2.4') == '2.4' else eu.PACKAGES_EMR_ALT
            package_str = ','.join(package)
            pac = [f"--packages={app_args.get('spark_packages')}"] if app_args.get('spark_packages') else [f"--packages={package_str}"]
            jar = [f"--jars={app_args.get('spark_jars')}"] if app_args.get('spark_jars') else [f"--jars={eu.JARS}"]
        else:
            pac = []
            jar = []
        med = ["--driver-memory={}".format(app_args['driver-memory'])] if app_args.get('driver-memory') else []
        cod = ["--driver-cores={}".format(app_args['driver-cores'])] if app_args.get('driver-cores') else []
        mee = ["--executor-memory={}".format(app_args['executor-memory'])] if app_args.get('executor-memory') else []
        coe = ["--executor-cores={}".format(app_args['executor-cores'])] if app_args.get('executor-cores') else []

        spark_app_args = [
            eu.CLUSTER_APP_FOLDER + launcher_file,
            "--mode={}".format(emr_mode),
            "--deploy=none",
            "--storage=s3",
            "--rerun_criteria={}".format(app_args.get('rerun_criteria')),
        ]
        jop = ['--job_param_file={}'.format(eu.CLUSTER_APP_FOLDER + eu.JOBS_METADATA_FILE)] if app_args.get('job_param_file') else []
        dep = ["--dependencies"] if app_args.get('dependencies') else []
        box = ["--chain_dependencies"] if app_args.get('chain_dependencies') else []
        sql = ["--sql_file={}".format(eu.CLUSTER_APP_FOLDER + app_args['sql_file'])] if app_args.get('sql_file') else []
        nam = ["--job_name={}".format(app_args['job_name'])] if app_args.get('job_name') else []

        return spark_submit_args + pac + jar + med + cod + mee + coe + spark_app_args + jop + dep + box + sql + nam

    def run_aws_data_pipeline(self):
        self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        # AWSDataPipeline ops
        client = self.session.client('datapipeline')
        self.deactivate_similar_pipelines(client, self.pipeline_name)
        pipe_id = self.create_data_pipeline(client)
        parameterValues = self.define_data_pipeline(client, pipe_id, self.emr_core_instances)
        self.activate_data_pipeline(client, pipe_id, parameterValues)

    def create_data_pipeline(self, client):
        unique_id = uuid.uuid1()
        create = client.create_pipeline(name=self.pipeline_name, uniqueId=str(unique_id))
        logger.debug('Pipeline created :' + str(create))

        pipe_id = create['pipelineId']  # format: 'df-0624751J5O10SBRYJJF'
        logger.info('Created pipeline with id ' + pipe_id)
        logger.debug('Pipeline description :' + str(client.describe_pipelines(pipelineIds=[pipe_id])))
        return pipe_id

    def define_data_pipeline(self, client, pipe_id, emr_core_instances):
        import awscli.customizations.datapipeline.translator as trans
        base = self.get_package_path()

        if emr_core_instances != 0:
            definition_file = base / 'yaetos/definition.json'  # see syntax in datapipeline-dg.pdf p285 # to add in there: /*"AdditionalMasterSecurityGroups": "#{}",  /* To add later to match EMR mode */
        else:
            definition_file = base / 'yaetos/definition_standalone_cluster.json'
            # TODO: have 1 json for both to avoid having to track duplication.

        definition = json.load(open(definition_file, 'r'))  # Note: Data Pipeline doesn't support emr-6.0.0 yet.

        pipelineObjects = trans.definition_to_api_objects(definition)
        parameterObjects = trans.definition_to_api_parameters(definition)
        parameterValues = trans.definition_to_parameter_values(definition)
        parameterValues = self.update_params(parameterValues)
        logger.debug(f'Filled pipeline with data from {definition_file}')

        response = client.put_pipeline_definition(
            pipelineId=pipe_id,
            pipelineObjects=pipelineObjects,
            parameterObjects=parameterObjects,
            parameterValues=parameterValues
        )
        logger.debug('put_pipeline_definition response: ' + str(response))
        return parameterValues

    def activate_data_pipeline(self, client, pipe_id, parameterValues):
        response = client.activate_pipeline(
            pipelineId=pipe_id,
            parameterValues=parameterValues,  # optional. If set, need to specify all params as per json.
            # startTimestamp=datetime(2018, 12, 1)  # optional
        )
        logger.debug('activate_pipeline response: ' + str(response))
        logger.info('Activated pipeline ' + pipe_id)

    def list_data_pipeline(self, client):
        out = client.list_pipelines(marker='')
        pipelines = out['pipelineIdList']
        while out['hasMoreResults'] is True:
            out = client.list_pipelines(marker=out['marker'])
            pipelines += out['pipelineIdList']
        return pipelines

    def deactivate_similar_pipelines(self, client, pipeline_id):
        pipelines = self.list_data_pipeline(client)
        for item in pipelines:
            job_name = self.get_job_name(item['name'])
            if job_name == self.app_args['job_name']:
                response = client.deactivate_pipeline(pipelineId=item['id'], cancelActive=True)
                response_code = response['ResponseMetadata']['HTTPStatusCode']
                if response_code == 200:
                    logger.info('Deactivated pipeline {}, {}, {}'.format(job_name, item['name'], item['id']))
                else:
                    raise Exception("Pipeline couldn't be deactivated. Error message: {}".format(response))

    def update_params(self, parameterValues):
        # TODO: check if easier/simpler to change values at the source json instead of a processed one.
        # Change key pair
        myScheduleType = {'EMR_Scheduled': 'cron', 'EMR_DataPipeTest': 'ONDEMAND'}[self.deploy_args.get('deploy')]
        myPeriod = self.deploy_args['frequency'] or '1 Day'
        if self.deploy_args['start_date'] and isinstance(self.deploy_args['start_date'], datetime):
            myStartDateTime = self.deploy_args['start_date'].strftime('%Y-%m-%dT%H:%M:%S')
        elif self.deploy_args['start_date'] and isinstance(self.deploy_args['start_date'], str):
            myStartDateTime = self.deploy_args['start_date'].format(today=datetime.today().strftime('%Y-%m-%d'))
        else:
            myStartDateTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        bootstrap = 's3://{}/setup_nodes.sh'.format(self.package_path_with_bucket)

        for ii, item in enumerate(parameterValues):
            if 'myEC2KeyPair' in item.values():
                parameterValues[ii] = {'id': u'myEC2KeyPair', 'stringValue': self.ec2_key_name}
            elif 'mySubnet' in item.values():
                parameterValues[ii] = {'id': u'mySubnet', 'stringValue': self.ec2_subnet_id}
            elif 'myPipelineLogUri' in item.values():
                parameterValues[ii] = {'id': u'myPipelineLogUri', 'stringValue': "s3://{}/{}/scheduled_run_logs/".format(self.s3_bucket_logs, self.metadata_folder)}
            elif 'myScheduleType' in item.values():
                parameterValues[ii] = {'id': u'myScheduleType', 'stringValue': myScheduleType}
            elif 'myPeriod' in item.values():
                parameterValues[ii] = {'id': u'myPeriod', 'stringValue': myPeriod}
            elif 'myStartDateTime' in item.values():
                parameterValues[ii] = {'id': u'myStartDateTime', 'stringValue': myStartDateTime}
            elif 'myBootstrapAction' in item.values():
                parameterValues[ii] = {'id': u'myBootstrapAction', 'stringValue': bootstrap}
            elif 'myTerminateAfter' in item.values():
                parameterValues[ii] = {'id': u'myTerminateAfter', 'stringValue': self.deploy_args.get('terminate_after', '180 Minutes')}
            elif 'myEMRReleaseLabel' in item.values():
                parameterValues[ii] = {'id': u'myEMRReleaseLabel', 'stringValue': self.emr_version}
            elif 'myMasterInstanceType' in item.values():
                parameterValues[ii] = {'id': u'myMasterInstanceType', 'stringValue': self.ec2_instance_master}
            elif 'myCoreInstanceCount' in item.values():
                parameterValues[ii] = {'id': u'myCoreInstanceCount', 'stringValue': str(self.emr_core_instances)}
            elif 'myCoreInstanceType' in item.values():
                parameterValues[ii] = {'id': u'myCoreInstanceType', 'stringValue': self.ec2_instance_slaves}

        # Change steps to include proper path
        setup_command = 's3://{s3_region}.elasticmapreduce/libs/script-runner/script-runner.jar,s3://{s3_tmp_path}/setup_master.sh,s3://{s3_tmp_path}'.format(s3_tmp_path=self.package_path_with_bucket, s3_region=self.s3_region)  # s3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://bucket-tempo/ex1_frameworked_job.arthur_user1.20181129.231423/setup_master.sh,s3://bucket-tempo/ex1_frameworked_job.arthur_user1.20181129.231423/
        spark_submit_command = 'command-runner.jar,' + ','.join([item.replace(',', '\\\,') for item in self.get_spark_submit_args(self.app_file, self.app_args)])   # command-runner.jar,spark-submit,--py-files,/home/hadoop/app/scripts.zip,--packages=com.amazonaws:aws-java-sdk-pom:1.11.760\\\\,org.apache.hadoop:hadoop-aws:2.7.0,/home/hadoop/app/jobs/examples/ex1_frameworked_job.py,--storage=s3  # instructions about \\\ part: https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-emractivity.html

        commands = [setup_command, spark_submit_command]
        mm = 0
        for ii, item in enumerate(parameterValues):
            if 'myEmrStep' in item.values() and mm < 2:  # TODO: make more generic and cleaner
                parameterValues[ii] = {'id': u'myEmrStep', 'stringValue': commands[mm]}
                mm += 1

        logger.debug('parameterValues after changes: ' + str(parameterValues))
        return parameterValues

    def run_aws_airflow(self):
        s3 = self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        fname = self.create_dags()
        self.upload_dags(s3, fname)

    def create_dags(self):
        """
        Create the .py dag file from job_metadata.yml info, based on a template in 'airflow_template.py'
        """

        # Set start_date, should be string evaluable in python, or string compatible with airflow
        start_input = self.deploy_args.get('start_date', '{today}T00:00:00+00:00')
        if '{today}' in start_input:
            start_date = start_input.replace('{today}', datetime.today().strftime('%Y-%m-%d'))
            start_date = f'dateutil.parser.parse("{start_date}")'
        elif start_input.startswith('{') and start_input.endswith('}'):
            start_date = start_input[1:-1]
        elif start_input == 'None':
            start_date = 'None'
        else:
            start_date = f'dateutil.parser.parse("{start_date}")'

        # Set schedule, should be string evaluable in python, or string compatible with airflow
        freq_input = self.deploy_args.get('frequency', '@once')
        if freq_input.startswith('{') and freq_input.endswith('}'):
            schedule = freq_input[1:-1]
        elif freq_input == 'None':
            schedule = 'None'
        else:
            schedule = f"'{freq_input}'"

        params = {
            'ec2_instance_slaves': self.ec2_instance_slaves,
            'emr_core_instances': self.emr_core_instances,
            'package_path_with_bucket': self.package_path_with_bucket,
            'cmd_runner_args': self.get_spark_submit_args(self.app_file, self.app_args),
            'pipeline_name': self.pipeline_name,
            'emr_version': self.emr_version,
            'ec2_instance_master': self.ec2_instance_master,
            'deploy_args': self.deploy_args,
            'ec2_key_name': self.ec2_key_name,
            'ec2_subnet_id': self.ec2_subnet_id,
            's3_bucket_logs': self.s3_bucket_logs,
            'metadata_folder': self.metadata_folder,
            'dag_nameid': self.app_args['job_name'].replace("/", "-"),
            'start_date': start_date,
            'schedule': schedule,
            'emails': self.deploy_args.get('emails', '[]'),
            'region': self.s3_region,
        }

        param_extras = {key: self.deploy_args[key] for key in self.deploy_args if key.startswith('airflow.')}

        content = get_template(params, param_extras)
        if not os.path.isdir(self.DAGS):
            os.mkdir(self.DAGS)

        job_dag_name = self.set_job_dag_name(self.app_args['job_name'])
        fname = self.DAGS / Pt(job_dag_name)

        os.makedirs(fname.parent, exist_ok=True)
        with open(fname, 'w') as file:
            file.write(content)
        return fname

    def set_job_dag_name(self, jobname):
        suffix = '_dag.py'
        if jobname.endswith('.py'):
            return jobname.replace('.py', '_py' + suffix)
        elif jobname.endswith('.sql'):
            return jobname.replace('.sql', '_sql' + suffix)
        else:
            return jobname + suffix

    def upload_dags(self, s3, fname_local):
        """
        Move the dag files to S3
        """
        job_dag_name = self.set_job_dag_name(self.app_args['job_name'])
        s3_dags = self.app_args['s3_dags'].replace('{root_path}', self.app_args['root_path'])
        s3_dags = CPt(s3_dags + '/' + job_dag_name)

        s3.Object(s3_dags.bucket, s3_dags.key)\
          .put(Body=open(str(fname_local), 'rb'), ContentType='text/x-sh')
        logger.info(f"Uploaded dag job files to path '{s3_dags}'")
        return True

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


def terminate(error_message=None):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


def deploy_standalone(job_args_update={}):
    # TODO: refactor below to use 'deploy' arg to trigger all deploy features, instead of new 'deploy_option' set below.
    job_args = {
        # --- regular job params ---
        'job_param_file': None,
        'mode': 'dev_EMR',
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
