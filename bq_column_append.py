# appends new column to existing table
from google.cloud import bigquery as bq

client = bq.Client.from_service_account_json("gcp-service-acct.json")

#set project & dataset defaults
project_id = 'lofty-dynamics-283618'
dataset_id = 'weather'
table_name = 'forecast_raw'
table_id = str(project_id+"."+dataset_id+"."+table_name)


table = client.get_table(table_id)
original_schema = table.schema
new_schema = original_schema[:] 

new_schema.append(bq.SchemaField('number',"INTEGER"))  #modify this for the columnn you wish to add. Add multiple rows for multiple columns
table.schema = new_schema

# updates schema in bq

table = client.update_table(table, ["schema"])  


