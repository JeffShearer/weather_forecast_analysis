# Version of dag compatible w/ Google Compose on GCP

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import urllib.parse

from sqlalchemy import values
#from airflow import DAG
#from airflow.providers.google.cloud.operators import bigquery
#from airflow.operators.python import PythonOperator


def get_forecasts():

    ## see https://www.weather.gov/documentation/services-web-api#/default/gridpoint_forecast for fine tuning these.
    response_api  = requests.get('https://api.weather.gov/gridpoints/SEW/130,123/forecast')
    data = response_api.text
    data = json.loads(data)
    #retrieves overall forecast generation date
    generated = data['properties']['generatedAt']
    #Retrieve just the forecasts for today - tomorrow night
    forecasts = data['properties']['periods'][:4]
    #appends forecast generation timestamp to every row
    for forecast in forecasts:
        forecast["generated"] = generated

    return forecasts

#forms bigquery-compatible query from observations & forecasts data
def insert_forecasts():
    q = get_forecasts()
     #retrieves field names for query
    columns = q[0].keys()
    columns = ",".join(columns)

    # retrieve values 
    values_list = []

    #loop through the values in each forecast entry, retrieve the into a comma separated string
    for forecast in q:
        row = []
        for v in forecast:
            #create list of dictionary values
            if isinstance(forecast[v],str):
                val = f"'{forecast[v]}'"
            else:
                 val = forecast[v]
            row.append(val) 
        values_list.append(row)


    #q = f"INSERT INTO `lofty-dynamics-283618.weather.forecast_raw`(collected,temp_today,temp_tonight,temp_tomorrow,temp_tomorrow_night)"
    #f"VALUES ('{q[0]['collected']}', {q[0]['temp_today']}, {q[0]['temp_tonight']}, {q[0]['temp_tomorrow']}, {q[0]['temp_tomorrow_night']})"
    return values_list
    

        
forecasts_query = insert_forecasts()

print(forecasts_query)

dag = DAG(
    'weather_dag',
    description='retrieve weather results',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,6,17,7,5,00),
    catchup=False
)

forecasts_task = bigquery.BigQueryInsertJobOperator(
        task_id='forecasts',
        dag=dag,
        configuration={
            "query": {
                "query": f"{forecasts_query}",
                "useLegacySql": False
            }
        }
    )

# define dag flow order of operations

forecasts_task