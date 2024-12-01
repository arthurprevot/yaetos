import json
import uuid
from datetime import datetime
from yaetos.logger import setup_logging
logger = setup_logging('Deploy')


class AWS_Data_Pipeliner():

    @staticmethod
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

    @staticmethod
    def create_data_pipeline(self, client):
        unique_id = uuid.uuid1()
        create = client.create_pipeline(name=self.pipeline_name, uniqueId=str(unique_id))
        logger.debug('Pipeline created :' + str(create))

        pipe_id = create['pipelineId']  # format: 'df-0624751J5O10SBRYJJF'
        logger.info('Created pipeline with id ' + pipe_id)
        logger.debug('Pipeline description :' + str(client.describe_pipelines(pipelineIds=[pipe_id])))
        return pipe_id

    @staticmethod
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

    @staticmethod
    def activate_data_pipeline(self, client, pipe_id, parameterValues):
        response = client.activate_pipeline(
            pipelineId=pipe_id,
            parameterValues=parameterValues,  # optional. If set, need to specify all params as per json.
            # startTimestamp=datetime(2018, 12, 1)  # optional
        )
        logger.debug('activate_pipeline response: ' + str(response))
        logger.info('Activated pipeline ' + pipe_id)

    @staticmethod
    def list_data_pipeline(self, client):
        out = client.list_pipelines(marker='')
        pipelines = out['pipelineIdList']
        while out['hasMoreResults'] is True:
            out = client.list_pipelines(marker=out['marker'])
            pipelines += out['pipelineIdList']
        return pipelines

    @staticmethod
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

    @staticmethod
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
