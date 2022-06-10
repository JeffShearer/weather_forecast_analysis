# Copy of actual dag in /dags - just for reference in github.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
from weather_forecast_analysis.observations import get_observations
from weather_forecast_analysis.forecasts import get_forecasts


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

get_observations = PythonOperator(
    task_id='get_observations',
    python_callable=get_observations,
    dag=dag,
)

get_forecasts = PythonOperator(
    task_id='get_forecasts',
    python_callable=get_forecasts,
    dag=dag,
)
end = EmptyOperator(task_id='end',dag=dag)

# define dag flow order of operations

start >> get_observations >> get_forecasts >> end