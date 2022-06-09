from google.cloud import bigquery as bq
from sqlalchemy import table
client = bq.Client.from_service_account_json("gcp-service-acct.json")

project_id = 'lofty-dynamics-283618'
dataset_id = 'weather'
table_name = 'forecast_raw'
table_id = str(project_id+"."+dataset_id+"."+table_name)


schema = [
    bq.SchemaField('id',"INTEGER"),
    bq.SchemaField('collected',"DATETIME"),
    bq.SchemaField('temp_today',"NUMERIC"),
    bq.SchemaField('temp_tonight',"NUMERIC"),
    bq.SchemaField('temp_tomorrow',"NUMERIC"),
    bq.SchemaField('temp_tomorrow_night',"NUMERIC"),
]
# For generating a new table

def createTable (table_id):
    forecast_table = bq.table.Table(table_id,schema=schema)
    forecast_table = client.create_table(table_id)


createTable(table_id)

# For modifying columns in existing table
#table_id = client.update_table(table_id, ["forecast_schema"])