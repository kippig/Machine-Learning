from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Temp").master("local[*]").enableHiveSupport().getOrCreate()

import pandas as pd
from datetime import datetime
from pytz import timezone
import sys

today = datetime.now(timezone('US/Pacific')).strftime("%A, %B %d, %Y")

locations = pd.read_csv(
    "file:///home/w205/w205_2017_final_project/wunderground_script_and_data/airport_locations_15.csv",
    index_col="airport")

airports = ['ATL','LAX','ORD','DFW','JFK','DEN','SFO','LAS',
            'CLT','SEA','PHX','MIA','MCO','IAH','EWR']

spark.sql("REFRESH TABLE delaypredictions")
spark.sql("REFRESH TABLE current_weather_new")
df = spark.sql("SELECT * FROM delaypredictions").toPandas()
weather = spark.sql("SELECT * FROM current_weather_new").toPandas()


df['Scheduled_Departure_Time'] = df[["Departure_hour","Departure_minute"]].apply(
        lambda x : '{:0>2d}:{:0>2d}'.format(x[0],x[1]), axis=1)

df["Probability_of_Delay"] = df['Delay_proba']

df["Departure_Airport"] = df['depAirport']
df["Arrival_Airport"] = df['arrAirport']

df = df.round({'Probability_of_Delay': 5})


# If 2 airport codes are input:

if len(sys.argv) == 3:
    dep = sys.argv[1]
    arr = sys.argv[2]

    dep_city = locations.loc[dep,'city']
    
    # For now just the top 15 airports are mapped to a city name:
    if arr in airports:
        arr_city = locations.loc[arr,'city']


    flights = df.loc[(df['depAirport'] == dep) & (df['arrAirport'] == arr)][[
                'Airline', 'Scheduled_Departure_Time', 'Probability_of_Delay',]]

    print "\nAll flights from {} to {} departing on {}:\n".format(dep, arr, today)

    # For a nice table display:
    spark_df = spark.createDataFrame(flights)
    spark.sql("DROP TABLE IF EXISTS mytemptable")
    spark_df.createOrReplaceTempView("mytempTable")
    spark.sql("SELECT * FROM mytemptable").show()
    spark.sql("DROP TABLE mytemptable")

    dep_w = weather.loc[weather['airport'] == dep]
    arr_w = weather.loc[weather['airport'] == arr]

    print "Today's weather conditions in {}:\n".format(dep_city)

    # convert units
    print 'Expected High Temp: ............. {} degrees F'.format(float(dep_w['tmax']) * 0.18 + 32)
    print 'Expected Low Temp: .............. {} degrees F'.format(float(dep_w['tmin']) * 0.18 + 32)
    print 'Expected Total Precipitation: ... {} mm'.format(float(dep_w['prcp']) / 10)
    print 'Expected Total Snowfall: ........ {} mm'.format(float(dep_w['snow']))

    try:
        print "\n\nToday's weather conditions in {}:\n".format(arr_city)
    except: 
        print "\n\n"
    else:
        print 'Expected High Temp: ............. {} degrees F'.format(float(arr_w['tmax']) * 0.18 + 32)
        print 'Expected Low Temp: .............. {} degrees F'.format(float(arr_w['tmin']) * 0.18 + 32)
        print 'Expected Total Precipitation: ... {} mm'.format(float(arr_w['prcp']) / 10)
        print 'Expected Total Snowfall: ........ {} mm\n\n'.format(float(arr_w['snow']))


# If 0 airport codes are input:

elif len(sys.argv) == 1:

    flights = df[['Departure_Airport', 'Arrival_Airport', 'Airline', 
                  'Scheduled_Departure_Time', 'Probability_of_Delay',]]

    print "\nAll flights currently being tracked:\n"

    # For a nice table display:
    spark_df = spark.createDataFrame(flights)
    spark.sql("DROP TABLE IF EXISTS mytemptable")
    spark_df.createOrReplaceTempView("mytempTable")
    spark.sql("SELECT * FROM mytemptable ORDER BY Probability_of_Delay DESC").show(100)
    spark.sql("DROP TABLE mytemptable")

