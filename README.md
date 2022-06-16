# weather_forecast_analysis
This is a practice project in building a full data pipeline with scheduled workflows for comparing weather forecast vs reality. 

Included are separate python scripts for retrieving weather data from the national weather service API, and then inserting that data as new rows in a biquery dataset.

Additionally there are scripts for initializing new bigquery tables and updating them

Finally, two versions of airflow DAGs are includeed, one for use in a locally run instance, and one that is compatible with GCP's cloud composer.

All of this is just a proof of concept that has been tested as working in both local & cloud environments, but as of this last update, no workflows are actively collecting any new data.
