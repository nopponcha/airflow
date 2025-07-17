from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='kubernetes_executor_example',
    default_args=default_args,
    description='DAG ที่ใช้ KubernetesPodOperator รันทุก 1 นาที',
    schedule_interval='* * * * *',  # ทุก 1 นาที
    start_date=datetime(2023, 1, 1),  # ตั้งเป็นอดีตแน่นอน
    catchup=False,
    tags=['k8s', 'minute'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "เริ่มต้น DAG ที่รันทุกนาที"',
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
