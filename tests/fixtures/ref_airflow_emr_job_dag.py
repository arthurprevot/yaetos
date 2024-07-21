
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator  # EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor  # EmrJobFlowSensor
from airflow.utils.dates import days_ago  # noqa: F401
from datetime import timedelta
import dateutil


DAG_ARGS = {
    'dag_id': 'ex-job_x',
    'dagrun_timeout': timedelta(hours=2),
    'start_date': dateutil.parser.parse("2024-07-15T00:00:00+00:00"),  # ignore_in_diff
    'schedule': '@once',
    'tags': ['emr'],
    'default_args': {
        'owner': 'me',
        'depends_on_past': False,
        'email': [],
        'email_on_failure': False,
        'email_on_retry': False,
    },
}


CLUSTER_JOB_FLOW_OVERRIDES = {
    'Name': 'yaetos__ex_s_job_x__20240101T000000',  # ignore_in_diff
    'ReleaseLabel': 'emr-6.1.1',
    'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Main nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },

            {
                'Name': 'Secondary nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': '2',
            }

        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2KeyName': 'to_be_filled',
        'Ec2SubnetId': 'to_be_filled',
        # 'AdditionalMasterSecurityGroups': extra_security_gp,  # TODO : make optional in future. "[self.extra_security_gp] if self.extra_security_gp else []" doesn't work.
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': "s3://mylake-dev/pipelines_metadata/manual_run_logs/",
    'BootstrapActions': [{
        'Name': 'setup_nodes',
        'ScriptBootstrapAction': {
            'Path': 's3n://mylake-dev/pipelines_metadata/jobs_code/yaetos__ex_s_job_x__20240701T000000/code_package/setup_nodes.sh',  # ignore_in_diff
            'Args': []
        }
    }],
    'Configurations': [
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
    ]
}

EMR_STEPS = [
    {
        'Name': 'Run Setup',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3://to_be_filled.elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': [
                "s3://mylake-dev/pipelines_metadata/jobs_code/yaetos__ex_s_job_x__20240701T000000/code_package/setup_master.sh",  # ignore_in_diff
                "s3://mylake-dev/pipelines_metadata/jobs_code/yaetos__ex_s_job_x__20240701T000000/code_package",  # ignore_in_diff
            ]
        }
    },
    {
        'Name': 'Spark Application',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--verbose', '--py-files=/home/hadoop/app/scripts.zip', '/home/hadoop/app/some/job.py', '--mode=None', '--deploy=none', '--storage=s3', '--job_name=ex/job_x'],
        },
    }
]

with DAG(**DAG_ARGS) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='start_emr_cluster',
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        job_flow_overrides=CLUSTER_JOB_FLOW_OVERRIDES
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=EMR_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}",  # [1] to watch 2nd step, the spark application.
        aws_conn_id='aws_default',
    )

    # # not used for now
    # cluster_checker = EmrJobFlowSensor(
    #     task_id='check_cluster',
    #     job_flow_id="{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}",
    #     aws_conn_id='aws_default',
    # )

    # # not used for now
    # terminate_cluster = EmrTerminateJobFlowOperator(
    #     task_id='terminate_cluster',
    #     job_flow_id="{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}",
    #     aws_conn_id='aws_default',
    # )

    cluster_creator >> step_adder >> step_checker
