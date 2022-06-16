# retrieves the coming forecast from NWS and appends to a bq table

import json
import requests
from google.cloud import bigquery as bq


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
    
    # big query append
    client = bq.Client.from_service_account_json("gcp-service-acct.json")
    table_id = 'lofty-dynamics-283618.weather.forecast_granular_raw'

    errors = client.insert_rows_json(table_id, forecasts)  # Make an API request.
    if errors == []:
       print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))



get_forecasts()