import os
from datetime import datetime
from pathlib import Path as Pt
from cloudpathlib import CloudPath as CPt
from yaetos.deploy_k8s import Kuberneter
from yaetos.deploy import terminate
from yaetos.logger import setup_logging
from yaetos.airflow_template import get_template
#from yaetos.airflow_template_k8s import get_template_k8s
logger = setup_logging('Deploy')


class Airflower():
    
    def run_aws_airflow(self):
        fname_local, job_dag_name = self.create_dags()

        s3 = self.s3_ops(self.session)
        if self.deploy_args.get('push_secrets', False):
            self.push_secrets(creds_or_file=self.app_args['connection_file'])  # TODO: fix privileges to get creds in dev env

        s3_dags = self.app_args.get('s3_dags')
        if s3_dags:
            self.upload_dags(s3, s3_dags, job_dag_name, fname_local)
        else:
            terminate(error_message='dag not uploaded, dag path not provided')

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

        # Get content
        if self.deploy_args['deploy'] == 'airflow':
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
                # airflow specific
                'dag_nameid': self.app_args['job_name'].replace("/", "-"),
                'start_date': start_date,
                'schedule': schedule,
                'emails': self.deploy_args.get('emails', '[]'),
                'region': self.s3_region,
            }
            param_extras = {key: self.deploy_args[key] for key in self.deploy_args if key.startswith('airflow.')}
            content = get_template(params, param_extras)
        elif self.deploy_args['deploy'] == 'airflow_k8s':
            content = Kuberneter().get_airflow_code(self, start_date, schedule)
        else:
            raise Exception("Should not get here")

        # Setup path
        default_folder = 'tmp/files_to_ship/dags'
        local_folder = Pt(self.app_args.get('local_dags', default_folder))
        if not os.path.isdir(local_folder):
            os.makedirs(local_folder, exist_ok=True)

        # Get fname_local
        job_dag_name = self.set_job_dag_name(self.app_args['job_name'])
        fname_local = local_folder / Pt(job_dag_name)

        # Write content to file
        os.makedirs(fname_local.parent, exist_ok=True)
        with open(fname_local, 'w') as file:
            file.write(content)
            logger.info(f'Airflow DAG file created at {fname_local}')

        return fname_local, job_dag_name

    def set_job_dag_name(self, jobname):
        suffix = '_dag.py'
        if jobname.endswith('.py'):
            return jobname.replace('.py', '_py' + suffix)
        elif jobname.endswith('.sql'):
            return jobname.replace('.sql', '_sql' + suffix)
        else:
            return jobname + suffix

    @staticmethod
    def upload_dags(s3, s3_dags, job_dag_name, fname_local):
        """
        Move the dag files to S3
        """
        s3_dags = CPt(s3_dags + '/' + job_dag_name)

        s3.Object(s3_dags.bucket, s3_dags.key)\
          .put(Body=open(str(fname_local), 'rb'), ContentType='text/x-sh')
        logger.info(f"Uploaded dag job files to path '{s3_dags}'")
        return True
