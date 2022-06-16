# version of forecasts.py with offline data pull for easier troubleshooting

import json
from google.cloud import bigquery as bq


def get_forecasts():
    # Testing with offline json file
    path = "forecast.sample.json"
    data = json.load(open(path, "r"))
    data = data['properties']
    generated = data['generatedAt']
    #Retrieve just the forecasts for today - tomorrow night
    forecasts = data['periods'][:4]

    for forecast in forecasts:
        #appends forecast generation timestamp to every row
        forecast["generated"] = generated

        # for future reference - this code will filter down the k-v pairs to a select few. 
        #revised = {key: forecast[key] for key in  forecast.keys() & {'number','startTime','endTime','temperature','shortForecast'}}
        #forecast_list.append(revised)

    
    # big query append
    client = bq.Client.from_service_account_json("gcp-service-acct.json")
    table_id = 'lofty-dynamics-283618.weather.forecast_granular_raw'


    errors = client.insert_rows_json(table_id, forecasts)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))



get_forecasts()