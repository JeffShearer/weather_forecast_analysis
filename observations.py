import json
import requests
import pandas as pd
import datetime
from google.cloud import bigquery as bq
client = bq.Client.from_service_account_json("dags/weather_forecast_analysis/gcp-service-acct.json")

def get_observations():
    table_id = 'lofty-dynamics-283618.weather.observations_raw'

    # pulls observations for bellingham airport for a given day
    response_api = requests.get('https://api.weather.gov/stations/KBLI/observations?start=2022-06-07T00%3A00%3A00%2B00%3A00&end=2022-06-08T00%3A00%3A00%2B00%3A00')
    data = response_api.text
    data = json.loads(data)
    data = data['features']

    # for testing only - dummy response data for troubleshooting
    #with open('dags/weather_forecast_analysis/sample-observation-response.json', 'r') as file: 
        #data = json.load(file)
        #data = data['features']
        
    #retrieve all non-null temperature observations for a day, put them in a list, and then calculate the average daily temp.
    generated = datetime.datetime.now()
    generated = generated.strftime("%Y-%m-%dT%H:%M:%S")
    temps_list = []
    for item in data:
        if item['properties']['temperature']['value'] is None:
                continue
        temps_list.append(item['properties']['temperature']['value'])

    avg_daily_temp = round(sum(temps_list)/len(temps_list),2)
    rows = [
        {"collected": generated, 
        "observations_count": len(temps_list), 
        "avg_daily_temp": avg_daily_temp}
    ]

    #Insert to BQ table
    errors = client.insert_rows_json(table_id, rows)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

