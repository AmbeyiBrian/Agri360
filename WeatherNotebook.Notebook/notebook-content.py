# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ce4b2f29-2557-4399-b70b-334be3743d1e",
# META       "default_lakehouse_name": "WeatherLakehouse",
# META       "default_lakehouse_workspace_id": "6ff4688f-2ca5-4c88-8ddd-3c4c7cc6ffca",
# META       "known_lakehouses": [
# META         {
# META           "id": "ce4b2f29-2557-4399-b70b-334be3743d1e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# Fetch data from Open-Meteo API
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": -1.2921,
    "longitude": 36.8219,
    "hourly": "temperature_2m,precipitation",
    "start_date": "2025-06-20",
    "end_date": "2025-06-21",
    "timezone": "Africa/Nairobi"
}
response = requests.get(url, params=params)
data = response.json()

# Flatten the hourly data
hourly = data["hourly"]
df = pd.DataFrame({
    "time": hourly["time"],
    "temperature_2m": hourly["temperature_2m"],
    "precipitation": hourly["precipitation"]
})

# Convert to Spark DataFrame
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)
spark_df = spark_df.withColumn("ingestion_time", current_timestamp())

# Save to Lakehouse table
spark_df.write.mode("overwrite").saveAsTable("weather_hourly")

print("✅ Weather data saved to Lakehouse table 'weather_hourly'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
