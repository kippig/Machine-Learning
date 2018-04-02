#!/bin/bash

# Save my current directory

MY_CWD=$(pwd)

# Clear the prior tracking data

echo "Cleaning directories"
rm -r /home/w205/Final_project_data/tracking/
mkdir /home/w205/Final_project_data/tracking/

# Fetch the current tracking data for the top 15 airports

echo "Fetching flight data"
python /home/w205/w205_2017_final_project/flightTracksUpdate.py

# Load all the flight data into spark and then save to Hive table

echo "Processing flight data"
spark-submit /home/w205/w205_2017_final_project/flightTracksHiveLoad.py

# Fetch the current weather data for the top 15 airports

echo "Fetching weather data"
spark-submit /home/w205/w205_2017_final_project/wunderground_script_and_data/get_weather_new.py

# Join flight and weather data and then make predictions with pickled ML model

echo "Joining data and predicting delays"
spark-submit /home/w205/w205_2017_final_project/MLPredict.py

# Return to the starting working directory

cd $MY_CWD

# Clean exit

exit
