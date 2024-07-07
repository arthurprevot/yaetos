"""
Function to create a generic airflow dag for EMR execution, generic enought to be used by any yaetos job.
Partly based on https://docs.aws.amazon.com/mwaa/latest/userguide/samples-emr.html
"""
from textwrap import dedent, indent


def get_template_k8s(params, param_extras):

    # params['KeepJobFlowAliveWhenNoSteps'] = params['deploy_args'].get('leave_on', False)


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
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
    from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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

    with DAG(**DAG_ARGS) as dag:

        spark_submit = SparkKubernetesOperator(
            task_id='spark_submit_task',
            namespace='a_k8s_namespace',
            application_file='spark-submit.yaml',
            kubernetes_conn_id='k8s_default',
            do_xcom_push=True,
        )

        spark_sensor = SparkKubernetesSensor(
            task_id='watch_step',
            namespace='a_k8s_namespace',
            application_name="{{{{ task_instance.xcom_pull(task_ids='spark_submit_task')['metadata']['name'] }}}}",
            kubernetes_conn_id='k8s_default',
            poke_interval=60,
            timeout=600,
        )

        # step_checker = EmrStepSensor(
        #     task_id='watch_step',
        #     job_flow_id="{{{{ task_instance.xcom_pull('start_emr_cluster', key='return_value') }}}}",
        #     step_id="{{{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}}}",  # [1] to watch 2nd step, the spark application.
        #     aws_conn_id='aws_default',
        # )

        # spark_submit
        spark_submit >> spark_sensor
    """.format(**params)
    return dedent(template)
