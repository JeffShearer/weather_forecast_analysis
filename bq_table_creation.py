from google.cloud import bigquery as bq

client = bq.Client.from_service_account_json("gcp-service-acct.json")

project_id = 'lofty-dynamics-283618'
dataset_id = 'weather'
table_name = 'forecast_granular_raw'
table_id = str(project_id+"."+dataset_id+"."+table_name)


schema = [
    bq.SchemaField('number',"INTEGER"),
    bq.SchemaField('name',"STRING"),
    bq.SchemaField('startTime',"STRING"),
    bq.SchemaField('endTime',"STRING"),
    bq.SchemaField('isDaytime',"BOOL"),
    bq.SchemaField('temperature',"INTEGER"),
    bq.SchemaField('temperatureUnit',"STRING"),
    bq.SchemaField('temperatureTrend',"STRING"),
    bq.SchemaField('windSpeed',"STRING"),
    bq.SchemaField('windDirection',"STRING"),
    bq.SchemaField('icon',"STRING"),
    bq.SchemaField('shortForecast',"STRING"),
    bq.SchemaField('detailedForecast',"STRING"),
    bq.SchemaField('generated',"STRING"),
   
]
# For generating a new table with provided schema

def createTable (table_id):
    table = bq.table.Table(table_id,schema=schema)
    table = client.create_table(table)

createTable(table_id)

