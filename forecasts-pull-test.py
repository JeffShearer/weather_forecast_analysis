import json
import requests




def get_forecasts():
    # Testing with offline json file
    path = "forecast.sample.json"
    data = json.load(open(path, "r"))
    data = data['properties']
    generated = data['generatedAt']
    forecasts = data['periods']

    forecast_list = []
    d = {}
    count = 0

    for forecast in forecasts:
        revised = {key: forecast[key] for key in  forecast.keys() & {'number','startTime','endTime','temperature','shortForecast'}}
        forecast_list.append(revised)

    print(forecast_list)




    #generated = data['generatedAt']
    # strips timezone so as not to piss off BQ
    #generated = generated[:generated.find('+')]
    #today_temp = data['periods'][0]['temperature']
    #tonight_temp = data['periods'][1]['temperature']
    #tomorrow_temp = data['periods'][2]['temperature']
    #tomorrow_night_temp = data['periods'][3]['temperature']

    
    #row = [
       # {"collected": generated, 
        #"temp_today": today_temp, 
        #"temp_tonight": tonight_temp,
        #"temp_tomorrow": tomorrow_temp,  
        #"temp_tomorrow_night": tomorrow_night_temp}
    #]

    #return data


get_forecasts()