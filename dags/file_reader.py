from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
        s3_bucket = 'input' 
        s3_conn_id = 's3'       
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if csv_files:
            for csv_file in csv_files:
                context['ti'].log.info(f"found file: {csv_file}")
                file_path = os.path.join(data_dir, csv_file)
                s3_hook.load_file(file_path, csv_file, s3_bucket)
                context['ti'].log.info(f"uploaded file: {csv_file} to bucket {s3_bucket}")
        else:
            context['ti'].log.info("no csv files found")



    list_files()