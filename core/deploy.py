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
import sys
from datetime import datetime
import time
import tarfile
import boto3
import botocore
from botocore.exceptions import ClientError
import uuid
import json
from configparser import ConfigParser
from shutil import copyfile
import core.etl_utils as eu
import core.logger as log


class DeployPySparkScriptOnAws(object):
    """
    Programmatically deploy a local PySpark script on an AWS cluster
    """
    SCRIPTS = 'core/scripts/' # TODO: move to etl_utils.py
    TMP = 'tmp/files_to_ship/'

    def __init__(self, yml, deploy_args, app_args):

        logger.info("etl deploy_args: {}".format(deploy_args))
        logger.info("etl app_args: {}".format(app_args))
        aws_setup = deploy_args['aws_setup']
        config = ConfigParser()
        assert os.path.isfile(deploy_args['aws_config_file'])
        config.read(deploy_args['aws_config_file'])

        self.app_file = yml.py_job  # TODO: remove all refs to app_file to be consistent.
        self.yml = yml
        self.aws_setup = aws_setup
        # self.app_name = self.yml.job_name.replace('.','_dot_').split('/')[-1]  # TODO: integrate in yml obj and use it here.
        self.ec2_key_name  = config.get(aws_setup, 'ec2_key_name')
        self.s3_region     = config.get(aws_setup, 's3_region')
        self.user          = config.get(aws_setup, 'user')
        self.profile_name  = config.get(aws_setup, 'profile_name')
        self.ec2_subnet_id = config.get(aws_setup, 'ec2_subnet_id')
        self.extra_security_gp = config.get(aws_setup, 'extra_security_gp')
        self.emr_core_instances = int(config.get(aws_setup, 'emr_core_instances'))
        self.app_args = app_args
        self.deploy_args = deploy_args
        self.ec2_instance_master = app_args.get('ec2_instance_master', 'm5.xlarge')  #'m5.12xlarge', # used m3.2xlarge (8 vCPU, 30 Gib RAM), and earlier m3.xlarge (4 vCPU, 15 Gib RAM)
        self.ec2_instance_slaves = app_args.get('ec2_instance_slaves', 'm5.xlarge')
        # Paths
        self.s3_bucket_logs = config.get(aws_setup, 's3_bucket_logs')
        self.job_name = self.generate_pipeline_name(self.yml.job_name, self.user)  #TODO: rename job_name to pipeline_name format: some_job.some_user.20181204.153429
        self.job_log_path = 'yaetos/logs/{}'.format(self.job_name)  # format: yaetos/logs/some_job.some_user.20181204.153429
        self.job_log_path_with_bucket = '{}/{}'.format(self.s3_bucket_logs, self.job_log_path)   # format: bucket-tempo/yaetos/logs/some_job.some_user.20181204.153429
        self.package_path  = self.job_log_path+'/package'   # format: yaetos/logs/some_job.some_user.20181204.153429/package
        self.package_path_with_bucket  = self.job_log_path_with_bucket+'/package'   # format: bucket-tempo/yaetos/logs/some_job.some_user.20181204.153429/package
        self.session = boto3.Session(profile_name=self.profile_name)  # aka AWS IAM profile

    def run(self):
        if self.app_args.get('mode')=='EMR':
            self.run_direct()
        elif self.app_args.get('mode') in ('EMR_Scheduled', 'EMR_DataPipeTest'):
            self.run_aws_data_pipeline()
        else:
            raise Exception("Shouldn't get here.")

    def run_direct(self):
        """Useful to run job on cluster without bothering with aws data pipeline. Also useful to add steps to existing cluster."""
        self.s3_ops(self.session)
        self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        # EMR ops
        c = self.session.client('emr')
        clusters = self.get_active_clusters(c)
        cluster = self.choose_cluster(clusters)
        new_cluster = cluster['id'] is None
        if new_cluster:
            print("Starting new cluster")
            self.start_spark_cluster(c)
            print("cluster name: %s, and id: %s"%(self.job_name, self.cluster_id))
            self.step_run_setup_scripts(c)
        else:
            print("Reusing existing cluster, name: %s, and id: %s"%(cluster['name'], cluster['id']))
            self.cluster_id = cluster['id']
            self.step_run_setup_scripts(c)

        # Run job
        self.step_spark_submit(c, self.app_file, self.app_args)

        # Clean
        if new_cluster and not self.deploy_args.get('leave_on') and self.app_args.get('clean_post_run'):  # TODO: add clean_post_run in input options.
            logger.info("New cluster setup to be deleted after job finishes.")
            self.describe_status_until_terminated(c)
            self.remove_temp_files(s3)  # TODO: remove tmp files for existing clusters too but only tmp files for the job

    def s3_ops(self, session):
        s3 = session.resource('s3')
        self.temp_bucket_exists(s3)
        self.tar_python_scripts()
        self.move_bash_to_local_temp()
        self.upload_temp_files(s3)

    def get_active_clusters(self, c):
        response = c.list_clusters(
            ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'],
            )
        clusters = [(ii+1, item['Id'],item['Name']) for ii, item in enumerate(response['Clusters'])]
        return clusters

    def choose_cluster(self, clusters, cluster_id=None):
        if len(clusters) == 0:
            print('No cluster found, will create a new one')
            return {'id': None,
                    'name': None}

        if cluster_id is not None:
            print('Cluster_id set by user to {}'.format(cluster_id))
            return {'id': cluster_id,
                    'name': None}

        clusters.append((len(clusters)+1, None, 'Create a new cluster'))
        print('Clusters found for AWS account "%s":'%(self.aws_setup))
        print('\n'.join(['[%s] %s'%(item[0], item[2]) for item in clusters]))
        answer = input('Your choice ? ')
        return {'id':clusters[int(answer)-1][1],
                'name':clusters[int(answer)-1][2]}

    @staticmethod
    def generate_pipeline_name(job_name, user):
        return "yaetos__{}__{}".format(
            job_name.replace('.','_d_').replace('/','_s_'),
            # user.replace('.','_'),
            datetime.now().strftime("%Y%m%dT%H%M%S"))

    @staticmethod
    def get_job_name(pipeline_name):
        return pipeline_name.split('__')[1].replace('_d_', '.').replace('_s_', '/') if '__' in pipeline_name else None

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
                terminate("Bucket for temporary files does not exist: "+self.s3_bucket_logs+' '+e.message)
            terminate("Error while connecting to temporary Bucket: "+self.s3_bucket_logs+' '+e.message)
        logger.info("S3 bucket for temporary files exists: "+self.s3_bucket_logs)

    def tar_python_scripts(self):
        """
        :return:
        """
        base = eu.LOCAL_APP_FOLDER
        # Create tar.gz file
        t_file = tarfile.open(self.TMP + "scripts.tar.gz", 'w:gz')

        # Add files
        t_file.add(self.app_args['job_param_file'], arcname=eu.JOBS_METADATA_FILE)

        # ./core files
        files = os.listdir(base+'core/')
        for f in files:
            t_file.add(base+'core/' + f, arcname='core/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        # ./libs files
        # TODO: get better way to walk down tree (reuse walk from below)
        files = os.listdir(base+'libs/')
        for f in files:
            t_file.add(base+'libs/' + f, arcname='libs/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        files = os.listdir(base+'libs/analysis_toolkit/')
        for f in files:
            t_file.add(base+'libs/analysis_toolkit/' + f, arcname='libs/analysis_toolkit/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        files = os.listdir(base+'libs/python_db_connectors/')
        for f in files:
            t_file.add(base+'libs/python_db_connectors/' + f, arcname='libs/python_db_connectors/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        # ./jobs files and folders
        # TODO: extract code below in external function.
        files = []
        for (dirpath, dirnames, filenames) in os.walk(self.app_args['jobs_folder']):
            for file in filenames:
                if file.endswith(".py") or file.endswith(".sql"):
                    path = os.path.join(dirpath, file)
                    dir_tar = dirpath[len(self.app_args['jobs_folder']):]
                    path_tar = os.path.join(eu.JOB_FOLDER, dir_tar, file)
                    files.append((path,path_tar))
        for f, f_arc in files:
            t_file.add(f, arcname=f_arc)

        # List all files in tar.gz
        for f in t_file.getnames():
            logger.debug("Added %s to tar-file" % f)
        t_file.close()

    def move_bash_to_local_temp(self):
        for item in ['setup_master.sh', 'setup_nodes.sh', 'terminate_idle_cluster.sh']:
            copyfile(eu.LOCAL_APP_FOLDER+self.SCRIPTS+item, self.TMP+item)

    def upload_temp_files(self, s3):
        """
        Move the PySpark + bash scripts to the S3 bucket we use to store temporary files
        :param s3:
        :return:
        """
        # Looping through all 4 steps below doesn't work (Fails silently) so done 1 by 1 below.
        s3.Object(self.s3_bucket_logs, self.package_path + '/setup_master.sh')\
          .put(Body=open(self.TMP+'setup_master.sh', 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/setup_nodes.sh')\
          .put(Body=open(self.TMP+'setup_nodes.sh', 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/terminate_idle_cluster.sh')\
          .put(Body=open(self.TMP+'terminate_idle_cluster.sh', 'rb'), ContentType='text/x-sh')
        s3.Object(self.s3_bucket_logs, self.package_path + '/scripts.tar.gz')\
          .put(Body=open(self.TMP+'scripts.tar.gz', 'rb'), ContentType='application/x-tar')
        logger.info("Uploaded job files (scripts.tar.gz, setup_master.sh, setup_nodes.sh, terminate_idle_cluster.sh) to bucket path '{}/{}'".format(self.s3_bucket_logs, self.package_path))
        return True

    def remove_temp_files(self, s3):
        """
        Remove Spark files from temporary bucket
        :param s3:
        :return:
        """
        bucket = s3.Bucket(self.s3_bucket_logs)
        for key in bucket.objects.all():
            if key.key.startswith(self.job_name) is True:
                key.delete()
                logger.info("Removed '{}' from bucket for temporary files".format(key.key))

    def start_spark_cluster(self, c):
        """
        :param c: EMR client
        :return:
        """
        emr_version = "emr-5.26.0" # emr-6.0.0 is latest as of june 2020, first with python3 by default but not supported by AWS Data Pipeline, emr-5.26.0 is latest as of aug 2019 # Was "emr-5.8.0", which was compatible with m3.2xlarge.
        response = c.run_job_flow(
            Name=self.job_name,
            LogUri="s3://{}/elasticmapreduce/".format(self.s3_bucket_logs),
            ReleaseLabel=emr_version,
            Instances={
                'InstanceGroups': [{
                    'Name': 'EmrMaster',
                    'InstanceRole': 'MASTER',
                    'InstanceType': self.ec2_instance_master,
                    'InstanceCount': 1,
                    }, {
                    'Name': 'EmrCore',
                    'InstanceRole': 'CORE',
                    'InstanceType': self.ec2_instance_slaves,
                    'InstanceCount': self.emr_core_instances,
                    }],
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': self.deploy_args.get('leave_on', False),
                'Ec2SubnetId': self.ec2_subnet_id,
                # 'AdditionalMasterSecurityGroups': self.extra_security_gp,  # TODO : make optional in future. "[self.extra_security_gp] if self.extra_security_gp else []" doesn't work.
            },
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            Configurations=[
                { # Section to force python3 since emr-5.x uses python2 by default.
                "Classification": "spark-env",
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}
                    }]
                },
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
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.cluster_id = response['JobFlowId']
        else:
            terminate("Could not create EMR cluster (status code {})".format(response_code))

        logger.info("Created Spark EMR cluster ({}) with cluster_id {}".format(emr_version, self.cluster_id))

    def describe_status_until_terminated(self, c):
        """
        :param c:
        :return:
        """
        print('Waiting for job to finish on cluster')
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.cluster_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
                print('Job is finished')
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
                'Name': 'run setup',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': [
                        "s3://{}/setup_master.sh".format(self.package_path_with_bucket),
                        "s3://{}".format(self.package_path_with_bucket),
                        ]
                    }
                }]
            )
        logger.info("Added step")
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
        logger.info("Added step 'spark-submit' with command line '{}'".format(cmd_runner_args))
        time.sleep(1)  # Prevent ThrottlingException

    def get_spark_submit_args(self, app_file, app_args):
        cmd_runner_args = [
            "spark-submit",
            "--driver-memory=12g", # TODO: this and extra spark config args should be fed through etl_utils.create_contexts()
            "--verbose",
            "--py-files={}scripts.zip".format(eu.CLUSTER_APP_FOLDER),
            eu.CLUSTER_APP_FOLDER+app_file if app_file.startswith(eu.JOB_FOLDER) else eu.CLUSTER_APP_FOLDER+eu.JOB_FOLDER+app_file,
            "--mode=local",
            "--storage=s3",
            '--job_param_file={}'.format(eu.CLUSTER_APP_FOLDER+eu.JOBS_METADATA_FILE),
            "--dependencies" if app_args.get('dependencies') else "",
            "--boxed_dependencies" if app_args.get('boxed_dependencies') else "",
            "--rerun_criteria={}".format(app_args.get('rerun_criteria')),
            "--sql_file={}".format(eu.CLUSTER_APP_FOLDER+app_args['sql_file']) if app_args.get('sql_file') else "",
            ]
        cmd_runner_args = [item for item in cmd_runner_args if item]
        return cmd_runner_args

    def run_aws_data_pipeline(self):
        self.s3_ops(self.session)
        self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        # AWSDataPipeline ops
        client = self.session.client('datapipeline')
        self.deactivate_similar_pipelines(client, self.job_name)
        pipe_id = self.create_data_pipeline(client)
        parameterValues = self.define_data_pipeline(client, pipe_id)
        self.activate_data_pipeline(client, pipe_id, parameterValues)

    def create_data_pipeline(self, client):
        unique_id = uuid.uuid1()
        create = client.create_pipeline(name=self.job_name, uniqueId=str(unique_id))
        logger.debug('Pipeline created :' + str(create))

        pipe_id = create['pipelineId']  # format: 'df-0624751J5O10SBRYJJF'
        logger.info('Created pipeline with id ' + pipe_id)
        logger.debug('Pipeline description :' + str(client.describe_pipelines(pipelineIds=[pipe_id])))
        return pipe_id

    def define_data_pipeline(self, client, pipe_id):
        import awscli.customizations.datapipeline.translator as trans

        definition_file = eu.LOCAL_APP_FOLDER+'core/definition.json'  # see syntax in datapipeline-dg.pdf p285 # to add in there: /*"AdditionalMasterSecurityGroups": "#{}",  /* To add later to match EMR mode */
        definition = json.load(open(definition_file, 'r')) # Note: Data Pipeline doesn't support emr-6.0.0 yet.

        pipelineObjects = trans.definition_to_api_objects(definition)
        parameterObjects = trans.definition_to_api_parameters(definition)
        parameterValues = trans.definition_to_parameter_values(definition)
        parameterValues = self.update_params(parameterValues)
        logger.info('Filled pipeline with data from '+definition_file)

        response = client.put_pipeline_definition(
            pipelineId=pipe_id,
            pipelineObjects=pipelineObjects,
            parameterObjects=parameterObjects,
            parameterValues=parameterValues
        )
        logger.info('put_pipeline_definition response: '+str(response))
        return parameterValues

    def activate_data_pipeline(self, client, pipe_id, parameterValues):
        response = client.activate_pipeline(
            pipelineId=pipe_id,
            parameterValues=parameterValues,  # optional. If set, need to specify all params as per json.
            # startTimestamp=datetime(2018, 12, 1)  # optional
        )
        logger.info('activate_pipeline response: '+str(response))
        logger.info('Activated pipeline ' + pipe_id)

    def list_data_pipeline(self, client):
        out = client.list_pipelines(marker='')
        pipelines = out['pipelineIdList']
        while out['hasMoreResults'] == True:
            out = client.list_pipelines(marker=out['marker'])
            pipelines += out['pipelineIdList']
        return pipelines

    def deactivate_similar_pipelines(self, client, pipeline_id):
        pipelines = self.list_data_pipeline(client)
        for item in pipelines:
            job_name = self.get_job_name(item['name'])
            if job_name == self.yml.job_name:
                response = client.deactivate_pipeline(pipelineId=item['id'], cancelActive=True)
                logger.info('Deactivated pipeline {}, {}, {}'.format(job_name, item['name'], item['id']))

    def update_params(self, parameterValues):
        # TODO: check if easier/simpler to change values at the source json instead of a processed one.
        # Change key pair
        myScheduleType = {'EMR_Scheduled': 'cron', 'EMR_DataPipeTest': 'ONDEMAND'}[self.app_args.get('mode')]
        myPeriod = self.yml.frequency or '1 Day'
        if self.yml.start_date and isinstance(self.yml.start_date, datetime):
            myStartDateTime = self.yml.start_date.strftime('%Y-%m-%dT%H:%M:%S')
        elif self.yml.start_date and isinstance(self.yml.start_date, str):
            myStartDateTime = self.yml.start_date.format(today=datetime.today().strftime('%Y-%m-%d'))
        else :
            myStartDateTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        # TODO: self.yml.start_date is taken from jobs_metadata_local.yml, instead of jobs_metadata.yml. Fix it
        bootstrap = 's3://{}/setup_nodes.sh'.format(self.package_path_with_bucket)

        for ii, item in enumerate(parameterValues):
            if 'myEC2KeyPair' in item.values():
                parameterValues[ii] = {'id': u'myEC2KeyPair', 'stringValue': self.ec2_key_name}
            elif 'mySubnet' in item.values():
                parameterValues[ii] = {'id': u'mySubnet', 'stringValue': self.ec2_subnet_id}
            elif 'myPipelineLogUri' in item.values():
                parameterValues[ii] = {'id': u'myPipelineLogUri', 'stringValue': "s3://"+self.s3_bucket_logs}
            elif 'myScheduleType' in item.values():
                parameterValues[ii] = {'id': u'myScheduleType', 'stringValue': myScheduleType}
            elif 'myPeriod' in item.values():
                parameterValues[ii] = {'id': u'myPeriod', 'stringValue': myPeriod}
            elif 'myStartDateTime' in item.values():
                parameterValues[ii] = {'id': u'myStartDateTime', 'stringValue': myStartDateTime}
            elif 'myBootstrapAction' in item.values():
                parameterValues[ii] = {'id': u'myBootstrapAction', 'stringValue': bootstrap}

        # Change steps to include proper path
        setup_command =  's3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://{s3_tmp_path}/setup_master.sh,s3://{s3_tmp_path}'.format(s3_tmp_path=self.package_path_with_bucket) # s3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://bucket-tempo/ex1_frameworked_job.arthur_user1.20181129.231423/setup_master.sh,s3://bucket-tempo/ex1_frameworked_job.arthur_user1.20181129.231423/
        spark_submit_command = 'command-runner.jar,' + ','.join(self.get_spark_submit_args(self.app_file, self.app_args))   # command-runner.jar,spark-submit,--py-files,/home/hadoop/app/scripts.zip,/home/hadoop/app/jobs/examples/ex1_frameworked_job.py,--storage=s3

        commands  = [setup_command, spark_submit_command]
        mm = 0
        for ii, item in enumerate(parameterValues):
            if 'myEmrStep' in item.values() and mm < 2:  # TODO: make more generic and cleaner
                parameterValues[ii] = {'id': u'myEmrStep', 'stringValue': commands[mm]}
                mm += 1

        logger.info('parameterValues after changes: '+str(parameterValues))
        return parameterValues

    def push_secrets(self, creds_or_file):
        session = boto3.Session(profile_name=self.profile_name)  # aka AWS IAM profile
        client = session.client('secretsmanager')

        file = open(creds_or_file, "r")
        content = file.read()
        file.close()

        try:
            response = client.create_secret(
                Name=eu.AWS_SECRET_ID,
                SecretString=content,
            )
            logger.debug('create_secret response: '+str(response))
            logger.info('Created aws secret, from {}, under secret_id:{}'.format(creds_or_file, eu.AWS_SECRET_ID))
        except client.exceptions.ResourceExistsException:
            response = client.put_secret_value(
                SecretId=eu.AWS_SECRET_ID,
                SecretString=content,
            )
            logger.debug('put_secret_value response: '+str(response))
            logger.info('Updated aws secret, from {}, under secret_id:{}'.format(creds_or_file, eu.AWS_SECRET_ID))

    def delete_secrets(self):
        """ To be used manually for now to free AWS resources. """
        session = boto3.Session(profile_name=self.profile_name)  # aka AWS IAM profile
        client = session.client('secretsmanager')

        response = client.delete_secret(
            SecretId=eu.AWS_SECRET_ID,
            # RecoveryWindowInDays=123,
            ForceDeleteWithoutRecovery=True
        )
        logger.debug('delete_secret response: '+str(response))
        logger.info('Deleted aws secret, secret_id:'+eu.AWS_SECRET_ID)
        print('delete_secret response: {}'.format(response))


def deploy_all_scheduled():

    def get_yml(args):
        meta_file = args.get('job_param_file', 'repo')
        if meta_file is 'repo':
            meta_file = eu.CLUSTER_APP_FOLDER+eu.JOBS_METADATA_FILE if args['storage']=='s3' else eu.JOBS_METADATA_LOCAL_FILE
        yml = eu.Job_Yml_Parser.load_meta(meta_file)
        logger.info('Loaded job param file: ' + meta_file)
        return yml

    def get_bool(prompt):
        while True:
            try:
               return {"":True, "y":True,"n":False}[input(prompt).lower()]
            except KeyError:
               print("Invalid input please enter y or n!")

    def validate_job(job):
        return get_bool('Want to schedule "{}" [Y/n]? '.format(job))

    # TODO: reuse etl_utils.py Commandliner/set_commandline_args() to have cleaner interface and proper default values.
    deploy_args = {'leave_on': False,
                   'aws_config_file':eu.AWS_CONFIG_FILE, # TODO: make set-able
                   'aws_setup':'dev'}
    app_args = {'mode':'EMR_Scheduled',
                'job_param_file': '/Users/aprevot/myTHNDocs/code_thn/datapipelines/conf/jobs_metadata.yml', # TODO: make set-able
                'job_param_file': 'conf/jobs_metadata.yml', # TODO: make set-able
                'boxed_dependencies': True,
                'dependencies': True,
                'storage': 'local',
                'jobs_folder': eu.JOB_FOLDER,  # TODO: make set-able
                'connection_file': eu.CONNECTION_FILE, # TODO: make set-able
                }

    yml = get_yml(app_args)
    pipelines = yml.keys()
    for pipeline in pipelines:
        job_yml = eu.Job_Yml_Parser(app_args)
        job_yml.set_job_params(job_name=pipeline)
        if not job_yml.frequency:
            continue

        run = validate_job(pipeline)
        if not run:
            continue

        DeployPySparkScriptOnAws(job_yml, deploy_args, app_args).run()


def terminate(error_message=None):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


logger = log.setup_logging('Deploy')


if __name__ == "__main__":
    # Deploying 1 job manually.
    # Use as standalone to push random python script to cluster.
    # TODO: fails to create a new cluster but works to add a step to an existing cluster.
    print('command line: ', ' '.join(sys.argv))
    job_name = sys.argv[1] if len(sys.argv) > 1 else 'examples/ex1_raw_job_cluster.py'  # TODO: move to 'jobs/examples/ex1_raw_job_cluster.py'
    class bag(object):
        pass
    job_yml = bag()
    job_yml.job_name = job_name
    job_yml.py_job = job_name # will add /home/hadoop/app/  # TODO: try later as better from cmdline.
    deploy_args = {'leave_on': True, 'aws_config_file':eu.AWS_CONFIG_FILE, 'aws_setup':'dev'}
    app_args = {'mode':'EMR'}
    DeployPySparkScriptOnAws(job_yml, deploy_args, app_args).run()

    # Show list of all jobs running.
    deployer = DeployPySparkScriptOnAws(job_yml, deploy_args, app_args)
    client = deployer.session.client('datapipeline')
    pipelines = deployer.list_data_pipeline(client)
    print('#--- pipelines: ', pipelines)

    # (Re)deploy schedule jobs
    deploy_all_scheduled()
