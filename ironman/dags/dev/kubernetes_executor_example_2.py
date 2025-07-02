from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='kubernetes_executor_example',
    default_args=default_args,
    description='Simple DAG using KubernetesExecutor and KubernetesPodOperator',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'k8s'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Start KubernetesExecutor DAG"',
    )

    # Task จะรันใน Pod แยกต่างหาก ด้วย KubernetesPodOperator
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
        bash_command='echo "Finished running DAG"',
    )

    start >> run_in_k8s >> finish
