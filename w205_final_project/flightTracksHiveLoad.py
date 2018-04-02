#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Build the spark session
spark = SparkSession.builder.appName("FlightTracking").master("local[2]").getOrCreate()

# Point to the tracking files and then read all of them into a spark object
filepath = "file:///home/w205/Final_project_data/tracking/"
files = spark.sparkContext.textFile(filepath)

# Build a dataframe from the files in directory
df1 = spark.read.json(files)

# Use explode() to expand the nested array for each flightID
df1 = df1.select(explode(df1.flightTracks).alias("tracks"))

# Peel off the pertinent fields from the raw dataframe and save to a new DF
tracks1 = df1.select(df1.tracks.flightId.alias("flightId"), df1.tracks.tailNumber.alias("Tail_Number"), df1.tracks.carrierFsCode.alias("Airline"), df1.tracks.departureAirportFsCode.alias("depAirport"), df1.tracks.arrivalAirportFsCode.alias("arrAirport"), df1.tracks.delayMinutes.alias("delayMinutes"), year(df1.tracks.departureDate.dateLocal).alias("Year"), month(df1.tracks.departureDate.dateLocal).alias("Month"), hour(df1.tracks.departureDate.dateLocal).alias("Departure_hour"), minute(df1.tracks.departureDate.dateLocal).alias("Departure_minute"))

# Save the new dataframe into a Hive table named flightStats
tracks1.write.mode('overwrite').saveAsTable("flightStats")

