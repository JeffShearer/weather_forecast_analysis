# weather_forecast_analysis
Practice building a full data pipeline for comparing weather forecast vs reality. 

## Plan
Set up workflows (Airflow or similar) to every day:
1. Retrieve from the NWS weather forecast for a given lat/lon
2. Retrieve the previous day's observations
3. Parse and cleanse resulting data into structured data frames
4. Append each day's results to an incremental dataset in Google Big Query
5. Build models to contruct tables that compare forecast vs reality over time across a variety of metrics - conditions, temperature, visibility, etc.
6. Analyze and construct live visualizations that present findings once enough data has accumulated.