import json
#import requests
import pandas as pd
import datetime
from google.cloud import bigquery as bq
client = bq.Client.from_service_account_json("gcp-service-acct.json")


table_id = 'lofty-dynamics-283618.weather.observations_raw'


# pulls observations for bellingham airport for a given day
#response_api = requests.get('https://api.weather.gov/stations/KBLI/observations?start=2022-06-07T00%3A00%3A00%2B00%3A00&end=2022-06-08T00%3A00%3A00%2B00%3A00')
#data = response_api.text
#data = json.loads(data)

## pretty prints json https://www.freecodecamp.org/news/python-parse-json-how-to-read-a-json-file/
    #print(json.dumps(data[1]['properties']['temperature']['value'],indent=4))

# for testing only - dummy response data for troubleshooting
with open('sample-observation-response.json', 'r') as file: 
    data = json.load(file)
    data = data['features']
    
#retrieve all non-null temperature observations for a day, put them in a list, and then calculate the average daily temp.
count = 0
generated = datetime.datetime.now()
generated = generated.strftime("%Y-%m-%dT%H:%M:%S")
temps_list = []
for item in data:
    if item['properties']['temperature']['value'] is None:
            continue
    count += 1
    temps_list.append(item['properties']['temperature']['value'])

avg_daily_temp = sum(temps_list)/count
rows = [
    {"collected": generated, 
    "observations_count": count, 
    "avg_daily_temp": avg_daily_temp}
]

#Insert to BQ table
errors = client.insert_rows_json(table_id, rows)  # Make an API request.
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))


