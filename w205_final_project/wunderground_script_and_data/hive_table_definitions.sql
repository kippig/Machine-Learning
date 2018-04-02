DROP TABLE locations;
CREATE EXTERNAL TABLE locations
(
	airport STRING,
	lat FLOAT,
	long FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
	"separatorChar" = ',',
	"quoteChar" = '"',
	"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/airport_locations';






DROP TABLE current_weather;
CREATE EXTERNAL TABLE current_weather
(
	airport STRING,
	date FLOAT,
	tmax FLOAT,
	tmin FLOAT,
	prcp FLOAT,
	snow FLOAT,
	snwd FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
	"separatorChar" = ',',
	"quoteChar" = '"',
	"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/current_weather_data';