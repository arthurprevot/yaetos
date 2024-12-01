import boto3
import json


class Gluer():

    # def upload_code(args):

    @staticmethod
    def create_glue_job(glue_client, args):
        # TODO: check glue_client.update_job() for when job already exists.
        response = glue_client.create_job(
            Name=args['jobname'],
            Role=args['role'],
            Command={
                "Name": "glueetl",
                "ScriptLocation": args['s3_code'],
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--SOURCE_PATH": args['source_path'],
                "--TARGET_PATH": args['target_path'],
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": args['temp_dir'],
            },
            MaxRetries=1,
            MaxCapacity=2
        )
        print(f'Sent glue creation call. response = {response}')

    @staticmethod
    def start_job_run(glue_client, args):
        response = glue_client.start_job_run(
            JobName=args['jobname'],
            Arguments={
                "--SOURCE_PATH": args['source_path'],
                "--TARGET_PATH": args['target_path'],
            }
        )
        print(f"Sent job run call. Run ID: {response["JobRunId"]} and response = {response}")

    @staticmethod
    def delete_glue_job(glue_client, job_name):
        response = glue_client.delete_job(JobName=job_name)
        print(f"Sent glue deletion call for Job '{job_name}'. response = {response}")

    @staticmethod
    def delete_glue_trigger_job(glue_client, trigger_name):
        response = glue_client.delete_trigger(Name=trigger_name)
        print(f"Sent glue trigger deletion call for '{trigger_name}'. response = {response}")

    @staticmethod
    def create_glue_trigger(glue_client, args):
        job_name = args['jobname']
        trigger_name = f"trigger_{job_name}"
        response = glue_client.create_trigger(
            Name=trigger_name,
            # Type="ON_DEMAND",
            Type="SCHEDULED",
            Schedule=args['cron'],  # ex: "cron(0 7 * * ? *)",
            # Type="CONDITIONAL",
            # Predicate={
            #     "Logical": "AND",
            #     "Conditions": [
            #         {"JobName": "JobA", "State": "SUCCEEDED"}
            #     ]
            # },
            Actions=[{"JobName": job_name}],
            StartOnCreation=False,
        )
        print(f"Sent Trigger creation call for Job '{job_name}', trigger '{trigger_name}' created. response={response}")

    @staticmethod
    def create_glue_trigger_state_agnostic(self, glue_client, args):
        job_name = args['jobname']
        trigger_name = f"Trigger_{job_name}"

        try:
            glue_client.get_trigger(Name=trigger_name)
            print(f"Trigger '{trigger_name}' already exists. Will be deleted and recreated")
            # TODO: check glue_client.update_trigger() for when trigger exists
            self.delete_glue_trigger_job(glue_client, trigger_name)
            self.create_glue_trigger(glue_client, args)
        except glue_client.exceptions.EntityNotFoundException:
            self.create_glue_trigger(glue_client, args)


if __name__ == "__main__":

    glue_client = boto3.client("glue")
    job_file = 'example_transformation'
    version = '_v1'
    args = {
        "jobname": f"{job_file}{version}",
        "source_path": "s3://qwer/asdf/",
        "target_path": "s3://qwer/zxcv/",
        'local_code': f"glue_jobs/{job_file}.py",
        's3_code': "s3://qwer/metadata_pipelines/code/glue-scripts/file.py",
        'cron': "cron(0 7 * * ? *)",
        "temp_dir": "s3://qwer/temp/",
        "role": "arn:aws:iam::xxx",
    }

    Gluer.delete_glue_job(glue_client, args)
    # Gluer.upload_code(args)
    Gluer.create_glue_job(glue_client, args)
    Gluer.start_glue_job(glue_client, args)
    Gluer.create_glue_trigger_state_agnostic(glue_client, args)
    Gluer.create_job_step_function(args)  
