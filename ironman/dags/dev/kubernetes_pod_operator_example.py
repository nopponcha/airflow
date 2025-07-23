from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s_models
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
}

# ปรับหน่วย memory จาก 5000M → 5000Mi เพื่อรองรับ Kubernetes
resources_args = {
    'request_memory': '256Mi',
    'limit_memory': '5000Mi',
    'request_cpu': '150m',
    'limit_cpu': '500m',
}

labels_args = {
    'app': 'airflow',
    'type': 'batch',
    'company': 'airflow',
    'project': 'ironman'
}

# กำหนด resource requirements จากตัวแปร
resource_requirements = k8s_models.V1ResourceRequirements(
    requests={
        "memory": resources_args["request_memory"],
        "cpu": resources_args["request_cpu"],
    },
    limits={
        "memory": resources_args["limit_memory"],
        "cpu": resources_args["limit_cpu"],
    },
)

with DAG(
    dag_id='kubernetes_pod_operator_example',
    default_args=default_args,
    description='Simple DAG using KubernetesPodOperator',
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
        namespace='ironman-dev',
        image='alpine:3.18',
        cmds=["sh", "-c"],
        arguments=["echo Hello from inside a Kubernetes Pod! && sleep 5"],
        labels=labels_args,
        container_resources=resource_requirements,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    finish = BashOperator(
        task_id='finish',
        bash_command='echo "DAG เสร็จสมบูรณ์"',
    )

    start >> run_in_k8s >> finish
