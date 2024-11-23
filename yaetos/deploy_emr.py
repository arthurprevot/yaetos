# import os
from yaetos.logger import setup_logging
logger = setup_logging('Deploy')


class EMRer():
    # def run_direct_k8s(self):

    # @staticmethod
    # def get_spark_submit_args_k8s(app_file, app_args):

    # def launch_spark_submit_k8s(self, cmdline):

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
