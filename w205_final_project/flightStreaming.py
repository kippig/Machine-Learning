# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FlightStreaming").master("local[*]").getOrCreate()


df = spark.read.json("file:///home/w205/w205_2017_final_project/FlightStats_sample_data/tracking/tracks_ATL.json")
trackSchema = df.schema
sdf = spark.readStream.schema(trackSchema).json("file:///tmp/streaming")

sTracks = sdf.select(explode(sdf.flightTracks).alias("tracks"))
sFlights = sTracks.select(sTracks.tracks.flightId.alias("flightId"), sTracks.tracks.tailNumber.alias("tailNumber"), sTracks.tracks.carrierFsCode.alias("carrier"), sTracks.tracks.departureAirportFsCode.alias("depAirport"), sTracks.tracks.arrivalAirportFsCode.alias("arrAirport"),sTracks.tracks.delayMinutes.alias("delayMinutes"),sTracks.tracks.positions.lat[0].alias("lat"), sTracks.tracks.positions.lon[0].alias("lon"))

fltCounts = sFlights.groupBy("depAirport").count()
query1 = fltCounts.writeStream.outputMode("complete").format("console").start()

delays = sFlights.groupBy("depAirport").avg("delayMinutes")
query2 = delays.writeStream.outputMode("complete").format("console").start()
query2.awaitTermination()



