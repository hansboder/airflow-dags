from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='file_reader',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['file-reader'],
    catchup=False
) as dag:

        
    @dag.task
    def list_files(**context):
        import os
        data_dir = "/app/weather-data"
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if csv_files:
            for csv_file in csv_files:
                context['ti'].log.info(f"found file: {csv_file}")
        else:
            context['ti'].log.info("no csv files found")



    list_files()