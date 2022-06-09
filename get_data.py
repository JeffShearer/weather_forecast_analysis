import json
import requests
import pandas as pd



## see https://www.weather.gov/documentation/services-web-api#/default/gridpoint_forecast for fine tuning these.
#forecast_response_api  = requests.get('https://api.weather.gov/gridpoints/SEW/130,123/forecast')
#forecast_data = forecast_response_api.text
#forecast_response_api.close()

# pulls observations for bellingham airport for a given day
#observation_response_api = requests.get('https://api.weather.gov/stations/KBLI/observations?start=2022-06-07T00%3A00%3A00%2B00%3A00&end=2022-06-08T00%3A00%3A00%2B00%3A00')
#observation_data = observation_response_api.text
#observation_data = json.loads(observation_data)

# for testing only - accessing response data for troubleshooting parsing
with open('sample-observation-response.json', 'r') as observation_file: 
    observation_data = json.load(observation_file)
    observation_data = observation_data['features']

    ## pretty prints json https://www.freecodecamp.org/news/python-parse-json-how-to-read-a-json-file/
    #print(json.dumps(observation_data[1]['properties']['temperature']['value'],indent=4))

    #simple loop to retrieve all non-null temperature observations for a day, put them in a list, and then find the average daily temp.
    observation_count = 0
    temps_list = []
    for item in observation_data:
        if item['properties']['temperature']['value'] is None:
                continue
        observation_count += 1
        temps_list.append(item['properties']['temperature']['value'])

    print(temps_list)
    print("total observations:", observation_count)
    print('average daily temp:',sum(temps_list)/observation_count)
    print()




