# weather_forecast_analysis
This is a practice project in building a full data pipeline for comparing weather forecast vs reality. 

Included are separate python scripts for retrieving weather data from the national weather service API, and then inserting that data as new rows in biquery dataset.

Additionally, there are scripts for initializing new bigquery tables and updating them

Finally there are  two versions of airflow DAGs, one for use in a locally run Airflow instance, and one that is compatible with GCP's cloud composer.

All of this is just a proof of concept that has been tested as working in both local & cloud environments, but as of this last update, no workflows are actively collecting any new data.
