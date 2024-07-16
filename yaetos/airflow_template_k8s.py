"""
Function to create a generic airflow dag for spark-submit to AWS EKS, generic enought to be used by any yaetos job.
"""
from textwrap import dedent


def get_template_k8s(params, param_extras):

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
    from airflow.utils.dates import days_ago  # noqa: F401
    from datetime import timedelta
    import dateutil


    DAG_ARGS = {{
        'dag_id': '{dag_nameid}',
        'dagrun_timeout': timedelta(hours=2),
        'start_date': {start_date},
        'schedule': {schedule},
        'tags': ['emr'],
        'default_args': {{
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
            namespace='{k8s_namespace}',
            application_file='{k8s_airflow_spark_submit_yaml}',
            kubernetes_conn_id='k8s_default',
            do_xcom_push=True,
        )

        spark_sensor = SparkKubernetesSensor(
            task_id='watch_step',
            namespace='{k8s_namespace}',
            application_name="{{{{ task_instance.xcom_pull(task_ids='spark_submit_task')['metadata']['name'] }}}}",
            kubernetes_conn_id='k8s_default',
            poke_interval=60,
            timeout=600,
        )

        spark_submit >> spark_sensor
    """.format(**params)
    return dedent(template)
