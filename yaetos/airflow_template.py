"""
Function to create a generic airflow dag for EMR execution, generic enought to be used by any yaetos job.
Partly based on https://docs.aws.amazon.com/mwaa/latest/userguide/samples-emr.html
"""
from textwrap import dedent, indent


def get_template(params, param_extras):

    params['KeepJobFlowAliveWhenNoSteps'] = params['deploy_args'].get('leave_on', False)

    instance_groups_extra = """
    {{
        'Name': 'Secondary nodes',
        'Market': 'ON_DEMAND',
        'InstanceRole': 'CORE',
        'InstanceType': '{ec2_instance_slaves}',
        'InstanceCount': '{emr_core_instances}',
    }}
    """.format(**params)

    instance_groups_extra = instance_groups_extra if params['emr_core_instances'] != 0 else ''
    params['instance_groups_extra'] = indent(instance_groups_extra, ' ' * 12)

    # Set extra params, params not available in template but overloadable
    lines = ''
    for item in param_extras.keys():
        entries = item.replace('airflow.', '').split('.')
        entries = '"]["'.join(entries)
        line = f'DAG_ARGS["{entries}"] = {param_extras[item]}\n' + ' ' * 4
        lines += line
    params['extras'] = lines

    template = """
    from airflow import DAG
    from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
    from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
    from airflow.utils.dates import days_ago
    import datetime
    from datetime import timedelta
    import dateutil
    import os


    DAG_ARGS = {{
        'dag_id': '{dag_nameid}',
        'dagrun_timeout': timedelta(hours=2),
        'start_date': {start_date},
        'schedule': {schedule},
        'tags': ['emr'],
        'default_args' : {{
            'owner': 'me',
            'depends_on_past': False,
            'email': {emails},
            'email_on_failure': False,
            'email_on_retry': False,
            }},
        }}
    {extras}

    CLUSTER_JOB_FLOW_OVERRIDES = {{
        'Name': '{pipeline_name}',
        'ReleaseLabel': '{emr_version}',
        'Applications': [{{'Name': 'Hadoop'}}, {{'Name': 'Spark'}}],
        'Instances': {{
            'InstanceGroups': [
                {{
                    'Name': "Main nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': '{ec2_instance_master}',
                    'InstanceCount': 1,
                }},
                {instance_groups_extra}
            ],
            'KeepJobFlowAliveWhenNoSteps': {KeepJobFlowAliveWhenNoSteps},
            'TerminationProtected': False,
            'Ec2KeyName': '{ec2_key_name}',
            'Ec2SubnetId': '{ec2_subnet_id}',
            # 'AdditionalMasterSecurityGroups': extra_security_gp,  # TODO : make optional in future. "[self.extra_security_gp] if self.extra_security_gp else []" doesn't work.
        }},
        'VisibleToAllUsers': True,
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'LogUri': "s3://{s3_bucket_logs}/{metadata_folder}/manual_run_logs/",
        'BootstrapActions': [{{
            'Name': 'setup_nodes',
            'ScriptBootstrapAction': {{
                'Path': 's3n://{package_path_with_bucket}/setup_nodes.sh',
                'Args': []
            }}
        }}],
        'Configurations': [
            {{  # Section to force python3 since emr-5.x uses python2 by default.
                "Classification": "spark-env",
                "Configurations": [{{
                    "Classification": "export",
                    "Properties": {{"PYSPARK_PYTHON": "/usr/bin/python3"}}
                }}]
            }},
            # {{ # Section to add jars (redshift...), not used for now, since passed in spark-submit args.
            # "Classification": "spark-defaults",
            # "Properties": {{ "spark.jars": ["/home/hadoop/redshift_tbd.jar"], "spark.driver.memory": "40G", "maximizeResourceAllocation": "true"}},
            # }}
        ]
    }}

    EMR_STEPS = [
        {{
            'Name': 'Run Setup',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {{
                'Jar': 's3://{region}.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': [
                    "s3://{package_path_with_bucket}/setup_master.sh",
                    "s3://{package_path_with_bucket}",
                ]
            }}
        }},
        {{
            'Name': 'Spark Application',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {{
                'Jar': 'command-runner.jar',
                'Args': {cmd_runner_args},
            }},
        }}
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
            job_flow_id="{{{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value') }}}}",
            aws_conn_id='aws_default',
            steps=EMR_STEPS,
        )

        step_checker = EmrStepSensor(
            task_id='watch_step',
            job_flow_id="{{{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}}}",
            step_id="{{{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}}}",  # [1] to watch 2nd step, the spark application.
            aws_conn_id='aws_default',
        )

        # # not used for now
        # cluster_checker = EmrJobFlowSensor(
        #     task_id='check_cluster',
        #     job_flow_id="{{{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}}}",
        #     aws_conn_id='aws_default',
        # )

        # # not used for now
        # terminate_cluster = EmrTerminateJobFlowOperator(
        #     task_id='terminate_cluster',
        #     job_flow_id="{{{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}}}",
        #     aws_conn_id='aws_default',
        # )

        cluster_creator >> step_adder >> step_checker
    """.format(**params)
    return dedent(template)
