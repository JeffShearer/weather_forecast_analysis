import json
import requests
import pandas as pd

## retrieves grid forecast, and puts payload into a variable
response_API  = requests.get('https://api.weather.gov/gridpoints/SEW/130,123/forecast')
data = response_API.text

print(data)



