from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime


default_args = {
    'owner': 'NamLe',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    dag_id='crawler_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tz='Asia/Bangkok'),
    schedule='0,10,20,30,40,50 * * * *',
    catchup=False,
    template_searchpath=['/opt/airflow']
) as dag:
    
    weather_crawler_task = DockerOperator(
        task_id='weather_crawler_task',
        image='crawler_image:latest',
        command='python3 -u weather_crawler/weather_crawler.py',
        network_mode='bigdata-net',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        env_file='crawler.env',
        dns=['8.8.8.8', '8.8.4.4'],
        mount_tmp_dir=False
    )

    pollution_crawler_task = DockerOperator(
        task_id='pollution_crawler_task',
        image='crawler_image:latest',
        command='python3 -u pollution_crawler/pollution_crawler.py',
        network_mode='bigdata-net',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        env_file='crawler.env',
        dns=['8.8.8.8', '8.8.4.4'],
        mount_tmp_dir=False
    )

    [weather_crawler_task, pollution_crawler_task]
