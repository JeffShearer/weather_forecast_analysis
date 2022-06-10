from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
sys.path.append('/mnt/c/users/me/synologyDrive/Learning/weather_forecast_analysis')
from observations import get_observations, test_observations


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,6,9,22,00,00),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='retrieve weather results, parse and pass to BQ',
    schedule_interval=timedelta(minutes=10),
)

start = EmptyOperator(task_id='start',dag=dag)

test_observations = PythonOperator(
    task_id='test_observations',
    python_callable=test_observations,
    dag=dag,
)

get_observations = PythonOperator(
    task_id='get_observations',
    python_callable=get_observations,
    dag=dag,
)
end = EmptyOperator(task_id='end',dag=dag)

start >> test_observations >> get_observations >> end