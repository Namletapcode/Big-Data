from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.bash import BashOperator
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
    schedule='5,35 * * * *',
    catchup=False,
    template_searchpath=['/opt/airflow']
) as dag:
    
    weather_crawler_task = KubernetesPodOperator(
        name='weather_crawler_task',
        namespace='default',
        image='crawler_image:1.0.0',
        in_cluster=True,
        cmds=['python3'],
        arguments=['-u', 'weather_crawler/weather_crawler.py'],
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy='Never'
    )

    pollution_crawler_task = KubernetesPodOperator(
        name='weather_crawler_task',
        namespace='default',
        image='crawler_image:1.0.0',
        in_cluster=True,
        cmds=['python3'],
        arguments=['-u', 'pollution_crawler/pollution_crawler.py'],
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy='Never'
    )

    [weather_crawler_task, pollution_crawler_task]

with DAG(
    dag_id='clean_old_file',
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tz='Asia/Bangkok'),
    schedule='0 0 * * *',
    catchup=False,
    template_searchpath=['/opt/airflow']
) as dag:
    
    clean_old_config_task = BashOperator(
        task_id='clean_old_config_task',
        bash_command='rm /opt/airflow/crawler/src/configs/weather_crawler_state.json'
    )
