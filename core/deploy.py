# encoding: utf-8
"""
Code running on client side to push code to AWS and execute it there.
Most of it from https://github.com/thomhopmans/themarketingtechnologist/tree/master/6_deploy_spark_cluster_on_aws
"""

# TODO:
# - use logger properly
# - get setup properly updated when changed and resubmitted to existing cluster (see current stderr output).

import logging
import os
import sys
from datetime import datetime
import time
import tarfile
import boto3
import botocore
from ConfigParser import ConfigParser
from shutil import copyfile
from etl_utils import JOBS_METADATA_FILE, CLUSTER_APP_FOLDER


class DeployPySparkScriptOnAws(object):
    """
    Programmatically deploy a local PySpark script on an AWS cluster
    """
    scripts = 'core/scripts/'
    tmp = 'tmp/files_to_ship/'

    def __init__(self, app_file, aws_setup='dev', **app_args):

        config = ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../conf/config.cfg'))

        self.app_file = app_file
        self.aws_setup = aws_setup
        self.app_name = self.app_file.replace('.py','').split('/')[-1]
        self.ec2_key_name = config.get(aws_setup, 'ec2_key_name')
        self.s3_bucket_logs = config.get(aws_setup, 's3_bucket_logs')
        self.s3_bucket_temp_files = config.get(aws_setup, 's3_bucket_temp_files')
        self.s3_region = config.get(aws_setup, 's3_region')
        self.user = config.get(aws_setup, 'user')
        self.profile_name = config.get(aws_setup, 'profile_name')
        self.emr_core_instances = int(config.get(aws_setup, 'emr_core_instances'))
        self.app_args = app_args

    def run(self):
        session = boto3.Session(profile_name=self.profile_name)  # aka AWS IAM profile

        # S3 ops
        s3 = session.resource('s3')
        self.temp_bucket_exists(s3)
        self.generate_job_name()
        self.tar_python_scripts()
        self.move_bash_to_local_temp()
        self.upload_temp_files(s3)

        # EMR ops
        c = session.client('emr')
        clusters = self.get_active_clusters(c)
        cluster = self.choose_cluster(clusters)
        new_cluster = cluster['id'] is None
        if new_cluster:
            print "Starting new cluster"
            self.start_spark_cluster(c)
            print "cluster name: %s, and id: %s"%(self.job_name, self.cluster_id)
        else:
            print "Reusing existing cluster, name: %s, and id: %s"%(cluster['name'], cluster['id'])
            self.cluster_id = cluster['id']
            self.step_run_setup_scripts(c)

        # Run job
        self.step_spark_submit(c, self.app_file, self.app_args)

        # Clean
        if new_cluster:
            self.describe_status_until_terminated(c)
            self.remove_temp_files(s3)  # TODO: remove tmp files for existing clusters too but only tmp files for the job

    def get_active_clusters(self, c):
        response = c.list_clusters(
            ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'],
            )
        clusters = [(ii+1, item['Id'],item['Name']) for ii, item in enumerate(response['Clusters'])]
        return clusters

    def choose_cluster(self, clusters, cluster_id=None):
        if len(clusters) == 0:
            print 'No cluster found, will create a new one'
            return {'id': None,
                    'name': None}

        if cluster_id is not None:
            print 'Cluster_id set by user to ', cluster_id
            return {'id': cluster_id,
                    'name': None}

        clusters.append((len(clusters)+1, None, 'Create a new cluster'))
        print 'Clusters found for AWS account "%s":'%(self.aws_setup)
        print '\n'.join(['[%s] %s'%(item[0], item[2]) for item in clusters])
        answer = raw_input('Your choice ? ')
        return {'id':clusters[int(answer)-1][1],
                'name':clusters[int(answer)-1][2]}

    def generate_job_name(self):
        self.job_name = "{}.{}.{}".format(self.app_name,
                                          self.user,
                                          datetime.now().strftime("%Y%m%d.%H%M%S"))

    def temp_bucket_exists(self, s3):
        """
        Check if the bucket we are going to use for temporary files exists.
        :param s3:
        :return:
        """
        try:
            s3.meta.client.head_bucket(Bucket=self.s3_bucket_temp_files)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                terminate("Bucket for temporary files does not exist: "+self.s3_bucket_temp_files+' '+e.message)
            terminate("Error while connecting to temporary Bucket: "+self.s3_bucket_temp_files+' '+e.message)
        logger.info("S3 bucket for temporary files exists: "+self.s3_bucket_temp_files)

    def tar_python_scripts(self):
        """
        :return:
        """
        # Create tar.gz file
        t_file = tarfile.open(self.tmp + "script.tar.gz", 'w:gz')

        # Add files
        t_file.add('__init__.py')
        t_file.add('conf/__init__.py')
        t_file.add(JOBS_METADATA_FILE)

        # ./core files
        files = os.listdir('core/')
        for f in files:
            t_file.add('core/' + f, filter=lambda obj: obj if obj.name.endswith('.py') else None)

        # ./jobs files and folder
        t_file.add('jobs/')

        # List all files in tar.gz
        for f in t_file.getnames():
            logger.info("Added %s to tar-file" % f)
        t_file.close()

    def move_bash_to_local_temp(self):
        for item in ['setup.sh', 'terminate_idle_cluster.sh']:
            copyfile(self.scripts+item, self.tmp+item)

    def upload_temp_files(self, s3):
        """
        Move the PySpark + bash scripts to the S3 bucket we use to store temporary files
        :param s3:
        :return:
        """
        # Shell file: setup (download S3 files to local machine)
        s3.Object(self.s3_bucket_temp_files, self.job_name + '/setup.sh')\
          .put(Body=open(self.tmp+'setup.sh', 'rb'), ContentType='text/x-sh')
        # Shell file: Terminate idle cluster
        s3.Object(self.s3_bucket_temp_files, self.job_name + '/terminate_idle_cluster.sh')\
          .put(Body=open(self.tmp+'terminate_idle_cluster.sh', 'rb'), ContentType='text/x-sh')
        # Compressed Python script files (tar.gz)
        s3.Object(self.s3_bucket_temp_files, self.job_name + '/script.tar.gz')\
          .put(Body=open(self.tmp+'script.tar.gz', 'rb'), ContentType='application/x-tar')
        logger.info("Uploaded files to key '{}' in bucket '{}'".format(self.job_name, self.s3_bucket_temp_files))
        return True

    def remove_temp_files(self, s3):
        """
        Remove Spark files from temporary bucket
        :param s3:
        :return:
        """
        bucket = s3.Bucket(self.s3_bucket_temp_files)
        for key in bucket.objects.all():
            if key.key.startswith(self.job_name) is True:
                key.delete()
                logger.info("Removed '{}' from bucket for temporary files".format(key.key))

    def start_spark_cluster(self, c):
        """
        :param c: EMR client
        :return:
        """
        emr_version = "emr-5.8.0"
        response = c.run_job_flow(
            Name=self.job_name,
            LogUri="s3://{}/elasticmapreduce/".format(self.s3_bucket_logs),
            ReleaseLabel=emr_version,
            Instances={
                'InstanceGroups': [{
                    'Name': 'EmrMaster',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 1,
                    }, {
                    'Name': 'EmrCore',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': self.emr_core_instances,
                    }],
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': False
            },
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            BootstrapActions=[{
                'Name': 'setup',
                'ScriptBootstrapAction': {
                    'Path': 's3n://{}/{}/setup.sh'.format(self.s3_bucket_temp_files, self.job_name),
                    'Args': ['s3://{}/{}'.format(self.s3_bucket_temp_files, self.job_name)]
                    }
                }, # {
                # 'Name': 'idle timeout',
                # 'ScriptBootstrapAction': {
                #     'Path':'s3n://{}/{}/terminate_idle_cluster.sh'.format(self.s3_bucket_temp_files, self.job_name),
                #     'Args': ['3600', '300']
                #     }
                # },
                ],
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
        print 'Waiting for job to finish on cluster'
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.cluster_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
                print 'Job is finished'
            logger.info(state)
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
                        "s3://{}/{}/setup.sh".format(self.s3_bucket_temp_files, self.job_name),
                        "s3://{}/{}".format(self.s3_bucket_temp_files, self.job_name),
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
        response = c.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[{
                'Name': 'Spark Application',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        "spark-submit",
                        "--py-files=%sscripts.zip"%CLUSTER_APP_FOLDER,
                        CLUSTER_APP_FOLDER+app_file,
                        "--execution=run",
                        "--storage=s3",
                        "--sql_file=%s"%(CLUSTER_APP_FOLDER+app_args['sql_file']) if app_args.get('sql_file') else "",  # TODO: better handling of app_args
                        "--dependencies" if app_args.get('dependencies') else "",  # TODO: better handling of app_args
                        ]
                    }
                }]
            )
        logger.info("Added step 'spark-submit' with argument '{}'".format(app_args))
        time.sleep(1)  # Prevent ThrottlingException


def setup_logging(default_level=logging.WARNING):
    """
    Setup logging configuration
    """
    logging.basicConfig(level=default_level)
    return logging.getLogger('DeployPySparkScriptOnAws')


def terminate(error_message=None):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


logger = setup_logging()

if __name__ == "__main__":
    print 'command line: ', ' '.join(sys.argv)
    app_file = sys.argv[1] if len(sys.argv) > 1 else 'jobs/examples/wordcount_frameworked.py'
    DeployPySparkScriptOnAws(app_file=app_file, aws_setup='perso').run()
