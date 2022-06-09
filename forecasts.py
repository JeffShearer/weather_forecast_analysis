import json
import requests
import pandas as pd
import datetime
from google.cloud import bigquery as bq
client = bq.Client.from_service_account_json("gcp-service-acct.json")

table_id = 'lofty-dynamics-283618.weather.forecast_raw'


## see https://www.weather.gov/documentation/services-web-api#/default/gridpoint_forecast for fine tuning these.
#forecast_response_api  = requests.get('https://api.weather.gov/gridpoints/SEW/130,123/forecast')
#forecast_data = forecast_response_api.textBQ
#print(json.dumps(forecast_data,indent=4))



## pretty prints json https://www.freecodecamp.org/news/python-parse-json-how-to-read-a-json-file/
    #print(json.dumps(observation_data[1]['properties']['temperature']['value'],indent=4))

# for testing only - dummy response data for troubleshooting
with open('sample-forecast-response.json','r') as file:
    data = json.load(file)
    data = data['properties']
    

# Captures timestamp of forecast collection time, and the temps for the first four entries in the forecast (today, tonight, tomorrow)
generated = data['generatedAt']
# strips timezone so as not to piss off BQ
generated = generated[:generated.find('+')]
today_temp = data['periods'][0]['temperature']
tonight_temp = data['periods'][1]['temperature']
tomorrow_temp = data['periods'][2]['temperature']
tomorrow_night_temp = data['periods'][3]['temperature']


rows = [
    {"collected": generated, 
    "temp_today": today_temp, 
    "temp_tonight": tonight_temp,
    "temp_tomorrow": tomorrow_temp,  
    "temp_tomorrow_night": tomorrow_night_temp}
]

#Insert to BQ table

errors = client.insert_rows_json(table_id, rows)  # Make an API request.
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))


