from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='kube_task',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['kubernetes-task'],
) as dag:

    kube_operator = KubernetesPodOperator(
        namespace='airflow',
        image="bash:devel-alpine3.20",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        name="echo",
        task_id="echo",
        get_logs=True
    )

kube_operator