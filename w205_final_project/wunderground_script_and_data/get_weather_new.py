#!/usr/bin/env python2
# -*- coding: utf-8 -*-


# Import libraries

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GetWeather").master("local[*]").enableHiveSupport().getOrCreate()

from urllib2 import urlopen
import json
import pandas as pd
import time 

# Wunderground API Key ID:
wg_api = "a183ff615ab25ad1"

# Load airport locations (latitude, longitude)
locations = pd.read_csv(
    "file:///home/w205/w205_2017_final_project/wunderground_script_and_data/airport_locations.csv", 
    index_col="airport")

# Get all 395 airports
# airports = locations.index.values.tolist()

# Use top 15 airports:
airports = ['ATL','LAX','ORD','DFW','JFK','DEN','SFO','LAS',
            'CLT','SEA','PHX','MIA','MCO','IAH','EWR']


# Create data frame structure to keep track of weather info:
df = pd.DataFrame(columns=["airport", "tmax", "tmin", "prcp", "snow"]) 

# Keep track of the airports with urlopen errors, timeouts or other errors to try again:
failures = []

def add_forecast(airport):
    '''Takes as input a 3-letter airport code
    '''
    try:
        f = urlopen('http://api.wunderground.com/api/{}/yesterday/forecast/q/{},{}.json'.\
                    format(wg_api, locations.loc[airport,'lat'], locations.loc[airport,'long']))
        json_string = f.read()
        parsed_json = json.loads(json_string)

        today = parsed_json['forecast']["simpleforecast"]["forecastday"][0]

        # Temps need to be in tenths of degrees C
        tmax = float(today['high']['celsius'])*10
        tmin = float(today['low']['celsius'])*10

        # Units for precipitation are in tenths of mm  
        prcp = float(today['qpf_allday']['mm'])*10

        # Units for snow are mm
        snow = float(today['snow_allday']['cm'])*10

        df.loc[df.shape[0]] = [airport, tmax, tmin, prcp, snow]

        f.close()
        time.sleep(12)  # Pauses 12 seconds after each call
    except:
        failures.append(airport)


# This should take about 3-4 mins:

# First pass
for airport in airports:
    add_forecast(airport)

# Try the airports that didn't work one more time:  
for i in range(len(failures)):
    add_forecast(failures.pop(0))

# Third and last pass
if failures:
    for i in range(len(failures)):
        add_forecast(failures.pop(0))


spark_df = spark.createDataFrame(df)

spark_df.createOrReplaceTempView("mytempTable") 

spark.sql("DROP TABLE IF EXISTS current_weather_new")

spark.sql("CREATE TABLE current_weather_new AS SELECT * FROM mytemptable")

spark.sql("DROP TABLE mytemptable")

