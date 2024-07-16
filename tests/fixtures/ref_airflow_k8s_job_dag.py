
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import datetime
from datetime import timedelta
import dateutil
import os


DAG_ARGS = {
    'dag_id': 'ex-job_x',
    'dagrun_timeout': timedelta(hours=2),
    'start_date': dateutil.parser.parse("2024-07-15T00:00:00+00:00"),  # ignore_in_diff
    'schedule': '@once',
    'tags': ['emr'],
    'default_args' : {
        'owner': 'me',
        'depends_on_past': False,
        'email': [],
        'email_on_failure': False,
        'email_on_retry': False,
        },
    }


with DAG(**DAG_ARGS) as dag:

    spark_submit = SparkKubernetesOperator(
        task_id='spark_submit_task',
        namespace='None',
        application_file='None',
        kubernetes_conn_id='k8s_default',
        do_xcom_push=True,
    )

    spark_sensor = SparkKubernetesSensor(
        task_id='watch_step',
        namespace='None',
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_task')['metadata']['name'] }}",
        kubernetes_conn_id='k8s_default',
        poke_interval=60,
        timeout=600,
    )

    spark_submit >> spark_sensor
