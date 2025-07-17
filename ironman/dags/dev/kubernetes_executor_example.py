from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='kubernetes_executor_example',
    default_args=default_args,
    description='Simple DAG using KubernetesExecutor (no extra providers)',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2025, 7, 17),
    catchup=False,
    tags=['example', 'k8s'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Start KubernetesExecutor DAG"',
    )

    # ใช้ BashOperator แทน KubernetesPodOperator ชั่วคราว
    run_in_k8s_simulated = BashOperator(
        task_id='run_in_k8s_simulated',
        bash_command='echo "Simulating task inside Kubernetes Pod (no KubernetesPodOperator)" && sleep 5',
    )

    finish = BashOperator(
        task_id='finish',
        bash_command='echo "Finished running DAG"',
    )

    start >> run_in_k8s_simulated >> finish
