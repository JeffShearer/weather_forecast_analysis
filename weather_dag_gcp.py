# Version of dag compatible w/ Google Compose on GCP

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import urllib.parse
from airflow import DAG
from airflow.providers.google.cloud.operators import bigquery
from airflow.operators.python import PythonOperator


def get_observations():
    station_id = "KBLI"
    # sets end date = midnight today, and start date = previous day at mindight 
    end_date = datetime.now() 
    start_date = end_date + timedelta(days=-1)
    start_date = start_date.strftime(f"%Y-%m-%dT00:00:00Z")
    end_date = end_date.strftime(f"%Y-%m-%dT00:00:00Z")

    #builds request url for weather.gov
    url = f'https://api.weather.gov/stations/{station_id}/observations?'
    params = {'start': start_date, 'end': end_date}
    url = url + urllib.parse.urlencode(params)
    
    # pulls observations for station forthe previous day
    response_api = requests.get(url)
    data = response_api.text
    data = json.loads(data)
    data = data['features']
        
    #retrieve all non-null temperature observations for a day, put them in a list, and then calculate the average daily temp.
    generated = datetime.now()
    generated = generated.strftime("%Y-%m-%dT%H:%M:%S")
    temps_list = []
    for item in data:
        if item['properties']['temperature']['value'] is None:
                continue
        temps_list.append(item['properties']['temperature']['value'])

    avg_daily_temp = round(sum(temps_list)/len(temps_list),2)
    row = [
        {"collected": generated, 
        "observations_count": len(temps_list), 
        "avg_daily_temp": avg_daily_temp}
    ]
    return row

def get_forecasts():

    ## see https://www.weather.gov/documentation/services-web-api#/default/gridpoint_forecast for fine tuning these.
    response_api  = requests.get('https://api.weather.gov/gridpoints/SEW/130,123/forecast')
    data = response_api.text
    data = json.loads(data)
    data = data['properties']

    # Captures timestamp of forecast collection time, and the temps for the first four entries in the forecast (today, tonight, tomorrow)
    generated = data['generatedAt']
    # strips timezone so as not to piss off BQ
    generated = generated[:generated.find('+')]
    today_temp = data['periods'][0]['temperature']
    tonight_temp = data['periods'][1]['temperature']
    tomorrow_temp = data['periods'][2]['temperature']
    tomorrow_night_temp = data['periods'][3]['temperature']

    
    row = [
        {"collected": generated, 
        "temp_today": today_temp, 
        "temp_tonight": tonight_temp,
        "temp_tomorrow": tomorrow_temp,  
        "temp_tomorrow_night": tomorrow_night_temp}
    ]

    return row

#forms bigquery-compatible query from observations & forecasts data
def insert_observations():
    o = get_observations()
    return f"INSERT INTO `lofty-dynamics-283618.weather.observations_raw`(collected,observations_count,avg_daily_temp) VALUES ('{o[0]['collected']}', {o[0]['observations_count']}, {o[0]['avg_daily_temp']})"

def insert_forecasts():
    f = get_forecasts()
    return f"INSERT INTO `lofty-dynamics-283618.weather.forecast_raw`(collected,temp_today,temp_tonight,temp_tomorrow,temp_tomorrow_night) VALUES ('{f[0]['collected']}', {f[0]['temp_today']}, {f[0]['temp_tonight']}, {f[0]['temp_tomorrow']}, {f[0]['temp_tomorrow_night']})"
observations_query = insert_observations()
forecasts_query = insert_forecasts()

dag = DAG(
    'weather_dag',
    description='retrieve weather results',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,6,16,00,00,00),
    catchup=False
)

observations_task = bigquery.BigQueryInsertJobOperator(
        task_id='observations',
        dag=dag,
        configuration={
            "query": {
                "query": f"{observations_query}",
                "useLegacySql": False
            }
        }
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

observations_task >> forecasts_task