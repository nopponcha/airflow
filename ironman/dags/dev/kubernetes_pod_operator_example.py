from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import configparser
import airflow.settings
from airflow.models import DagModel
import os
from typing import Dict
import importlib.util
from airflow.operators.python import get_current_context
from kubernetes.client import models as k8s_models
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import pytz

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='kubernetes_pod_operator_example',
    default_args=default_args,
    description='Simple DAG using kubernetesPodOperator',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['k8s', 'minute'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Start KubernetesExecutor DAG"',
    )

    run_in_k8s = KubernetesPodOperator(
        task_id='run_in_kubernetes',
        name='run-in-kubernetes',
        namespace='airflow',
        image='alpine:3.18',
        cmds=["sh", "-c"],
        arguments=["echo Hello from inside a Kubernetes Pod! && sleep 5"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    finish = BashOperator(
        task_id='finish',
        bash_command='echo "DAG เสร็จสมบูรณ์"',
    )

    start >> run_in_k8s >> finish
