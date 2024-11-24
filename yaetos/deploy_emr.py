import time
import botocore
from yaetos.deploy_utils import terminate
from yaetos.logger import setup_logging
logger = setup_logging('Deploy')


class EMRer():

    @staticmethod
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
            try:
                self.step_run_setup_scripts(c)
            except botocore.exceptions.ClientError as e:
                self.describe_status(c)
                logger.error(f"botocore.exceptions.ClientError : {e}")
                raise
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

    @staticmethod
    def get_active_clusters(self, c):
        response = c.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'],
        )
        clusters = [(ii + 1, item['Id'], item['Name']) for ii, item in enumerate(response['Clusters'])]
        return clusters

    @staticmethod
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
            Applications=self.emr_applications,  # should be at a minimum [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
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
            JobFlowRole=self.emr_ec2_role,
            ServiceRole=self.emr_role,
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

    @staticmethod
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

    @staticmethod
    def describe_status(self, c):
        description = c.describe_cluster(ClusterId=self.cluster_id)
        logger.info(f'Cluster description: {description}')

    @staticmethod
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

    @staticmethod
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