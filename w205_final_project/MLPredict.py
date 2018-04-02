#Import modules
from sklearn.linear_model import SGDClassifier
from sklearn.externals import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import unicodedata
from pyspark.sql import SparkSession, HiveContext

# Build the spark session
spark = SparkSession.builder.appName("DelayPrediction").master("local[*]").enableHiveSupport().getOrCreate()

# Join the current weather and flight data on departure airport in a spark dataframe
joined_df = spark.sql("SELECT * FROM flightstats f JOIN current_weather_new w ON f.depAirport=w.airport")

# Convert spark dataframe to pandas
df = joined_df.toPandas()
df['TMAX'] = pd.to_numeric(df['tmax'])
df['TMIN'] = pd.to_numeric(df['tmin'])
df['PRCP'] = pd.to_numeric(df['prcp'])
df['SNOW'] = pd.to_numeric(df['snow'])

# Data cleaning steps
df = df.dropna(subset = ['Tail_Number', 'depAirport', 'arrAirport'])
df = df[pd.notnull(df['Tail_Number'])]

df['Tail_Number'] = df['Tail_Number'].map(lambda x : unicodedata.normalize('NFKD', x).encode('ascii','ignore'))
df['Airline'] = df['Airline'].map(lambda x : unicodedata.normalize('NFKD', x).encode('ascii','ignore'))
df['depAirport'] = df['depAirport'].map(lambda x : unicodedata.normalize('NFKD', x).encode('ascii','ignore'))
df['arrAirport'] = df['arrAirport'].map(lambda x : unicodedata.normalize('NFKD', x).encode('ascii','ignore'))

#Domestic Airports only
airline = ['TZ', 'FL', 'AS', 'AQ', 'HP', 'AA', 'MQ', 'DH', 'EV', 'CO', 'DL', '9E', 'MQ', 'EV', 'XE', 'F9', 'HA', 'DH', 'B6', 'YV', 'NW', '9E', 'OO', 'WN', 'NK', 'US', 'UA', 'VX']
airports = ['ADK', 'ANC', 'ANI', 'BRW', 'BET', 'CDV', 'SCC', 'DLG', 'FAI', 'GST', 'JNU', 'KTN', 'AKN', 'ADQ', 'OTZ', 'OME', 'PSG', 'SIT', 'KSM', 'DUT', 'WRG', 'YAK', 'BHM', 'DHN', 'HSV', 'MOB', 'MGM', 'XNA', 'FSM', 'LIT', 'TXK', 'PPG', 'IFP', 'FLG', 'GCN', 'PHX', 'TUS', 'ACV', 'BFL', 'BUR', 'CIC', 'CCR', 'CEC', 'FAT', 'IPL', 'IYK', 'LGB', 'LAX', 'MMH', 'MOD', 'MRY', 'OAK', 'ONT', 'OXR', 'PSP', 'PMD', 'RDD', 'SMF', 'SAN', 'SFO', 'SJC', 'SBP', 'SNA', 'SBA', 'SMX', 'STS', 'TVL', 'SCK', 'VIS', 'ASE', 'COS', 'DEN', 'DRO', 'EGE', 'GJT', 'GUC', 'MTJ', 'PUB', 'TEX', 'HVN', 'BDL', 'DCA', 'IAD', 'ILG', 'DAB', 'FLL', 'RSW', 'GNV', 'JAX', 'EYW', 'MTH', 'MLB', 'MIA', 'APF', 'MCO', 'ECP', 'PNS', 'PGD', 'SRQ', 'PIE', 'TLH', 'TPA', 'VPS', 'PBI', 'ABY', 'ATL', 'AGS', 'BQK', 'CSG', 'MCN', 'SAV', 'VLD', 'GUM', 'ITO', 'HNL', 'OGG', 'KOA', 'MKK', 'LNY', 'LIH', 'CID', 'DSM', 'DBQ', 'SUX', 'ALO', 'BOI', 'SUN', 'IDA', 'LWS', 'PIH', 'TWF', 'BMI', 'CMI', 'MDW', 'ORD', 'RFD', 'MLI', 'PIA', 'SPI', 'EVV', 'FWA', 'IND', 'SBN', 'GCK', 'HYS', 'MHK', 'FOE', 'ICT', 'CVG', 'LEX', 'SDF', 'PAH', 'AEX', 'BTR', 'LFT', 'LCH', 'MLU', 'MSY', 'SHV', 'BOS', 'HYA', 'ACK', 'MVY', 'ORH', 'BWI', 'BGR', 'PWM', 'APN', 'DET', 'DTW', 'ESC', 'FNT', 'GRR', 'CMX', 'IMT', 'AZO', 'LAN', 'MKG', 'PLN', 'MBS', 'CIU', 'TVC', 'BJI', 'BRD', 'DLH', 'HIB', 'INL', 'MSP', 'RST', 'STC', 'COU', 'JLN', 'MKC', 'MCI', 'SGF', 'STL', 'CBM', 'GTR', 'GLH', 'GPT', 'PIB', 'JAN', 'MEI', 'TUP', 'BIL', 'BZN', 'BTM', 'GTF', 'HLN', 'GPI', 'MSO', 'WYS', 'AVL', 'CLT', 'FAY', 'GSO', 'HKY', 'OAJ', 'ISO', 'EWN', 'SOP', 'RDU', 'ILM', 'BIS', 'DVL', 'DIK', 'FAR', 'RDR', 'GFK', 'JMS', 'MIB', 'MOT', 'ISN', 'GRI', 'LNK', 'LBF', 'OMA', 'BFF', 'MHT', 'ACY', 'EWR', 'TTN', 'ABQ', 'FMN', 'HOB', 'ROW', 'SAF', 'EKO', 'LAS', 'RNO', 'ALB', 'BGM', 'BUF', 'ELM', 'ITH', 'JFK', 'LGA', 'ISP', 'SWF', 'IAG', 'PBG', 'ROC', 'SYR', 'ART', 'HPN', 'CAK', 'CLE', 'CMH', 'DAY', 'TOL', 'LAW', 'OKC', 'TUL', 'EUG', 'LMT', 'MFR', 'OTH', 'PDX', 'RDM', 'SLE', 'ABE', 'ERI', 'MDT', 'LBE', 'PHL', 'PIT', 'AVP', 'BQN', 'MAZ', 'PSE', 'SJU', 'PVD', 'CHS', 'CAE', 'FLO', 'GSP', 'MYR', 'ABR', 'PIR', 'RCA', 'RAP', 'FSD', 'TRI', 'CHA', 'TYS', 'MEM', 'BNA', 'ABI', 'AMA', 'AUS', 'BPT', 'BRO', 'CLL', 'CRP', 'DAL', 'TKI', 'DFW', 'DRT', 'ELP', 'GRK', 'HRL', 'EFD', 'IAH', 'HOU', 'ILE', 'LRD', 'GGG', 'LBB', 'MFE', 'MAF', 'SJT', 'SAT', 'TYR', 'VCT', 'ACT', 'SPS', 'CDC', 'CNY', 'OGD', 'PVU', 'SLC', 'SGU', 'VEL', 'ENV', 'CHO', 'LYH', 'PHF', 'ORF', 'RIC', 'ROA', 'SHD', 'STT', 'STX', 'BTV', 'BLI', 'MWH', 'PSC', 'BFI', 'SEA', 'SKA', 'GEG', 'YKM', 'ATW', 'EAU', 'GRB', 'LSE', 'MSN', 'MKE', 'CWA', 'RHI', 'CRW', 'CKB', 'HTS', 'LWB', 'CPR', 'CYS', 'COD', 'GCC', 'JAC', 'LAR', 'RKS', 'FCA', 'PFN', 'BKG', 'CLD', 'HDN', 'HHH', 'MQT', 'AZA', 'ROP', 'SPN', 'UST', 'SCE', 'UTM', 'YAP', 'YUM']


# Data is now in a pd.dataframe with columns matching the ML model 
#["Airline", "Year", "Month", "Tail_Number", "depAirport", "arrAirport",  "Departure_hour", "Departure_minute", 'PRCP', 'SNOW', 'TMAX', 'TMIN']

# Load pickled models for label encoders and logistic regression model
airportEncoder = joblib.load("/home/w205/w205_2017_final_project/Final_ML_Pieces/Airport_Encoder.pkl")
tailNumberEncoder = joblib.load("/home/w205/w205_2017_final_project/Final_ML_Pieces/Tail_Number_Encoder.pkl")
carrierEncoder = joblib.load("/home/w205/w205_2017_final_project/Final_ML_Pieces/Airline_Encoder.pkl")
model = joblib.load("/home/w205/w205_2017_final_project/Final_ML_Pieces/Batch_trained_model3.pkl")


# Process data to get into format for ML model
df = df[df['arrAirport'].isin(airports)]
finalDf = df.copy()
finalDf['Airline'] = finalDf['Airline'].map(lambda x: x if x in airline else '9E')
# unknown airlines map to the same place
finalDf['Airline'] = carrierEncoder.transform(finalDf['Airline'])
finalDf['depAirport'] = airportEncoder.transform(finalDf['depAirport'])
finalDf['arrAirport']  = airportEncoder.transform(finalDf['arrAirport'])
finalDf['Tail_Number'] = 0  #Just for now
# finalDf['Tail_Number'] = tailNumberEncoder.transform(finalDf['Tail_Number'])

# Units are agreed upon and need not be transformed.
# PRCP is in 10ths mm
# SNOW is in mm
# TMAX is in 10ths of a degree C
# TMIN is in 10ths of a degree C


# Normalization
minimums = {"Airline": 0.0, "Year": 2005, "Month": 1, "Tail_Number": 0, "depAirport": 0, "arrAirport": 0,  
           "Departure_hour": 0, "Departure_minute": 0, 'PRCP': 0, 'SNOW': 0, 'TMAX': -367, 'TMIN': -439}

maximums = {"Airline": 23, "Year": 2017, "Month": 12, "Tail_Number": 9128, "depAirport": 395, "arrAirport": 395,  
           "Departure_hour": 23, "Departure_minute": 59, 'PRCP': 3335, 'SNOW': 465, 'TMAX': 578, 'TMIN': 556}


finalDf['Airline'] = (finalDf['Airline'] - minimums['Airline'])/(maximums['Airline'] - minimums['Airline'])
finalDf['Year'] = (finalDf['Year'] - minimums['Year'])/(maximums['Year'] - minimums['Year'])
finalDf['Month'] = (finalDf['Month'] - minimums['Month'])/(maximums['Month'] - minimums['Month'])
finalDf['Tail_Number'] = (finalDf['Tail_Number'] - minimums['Tail_Number'])/(maximums['Tail_Number'] - minimums['Tail_Number'])
finalDf['depAirport'] = (finalDf['depAirport'] - minimums['depAirport'])/(maximums['depAirport'] - minimums['depAirport'])
finalDf['arrAirport'] = (finalDf['arrAirport'] - minimums['arrAirport'])/(maximums['arrAirport'] - minimums['arrAirport'])
finalDf['Departure_hour'] = (finalDf['Departure_hour'] - minimums['Departure_hour'])/(maximums['Departure_hour'] - minimums['Departure_hour'])
finalDf['Departure_minute'] = (finalDf['Departure_minute'] - minimums['Departure_minute'])/(maximums['Departure_minute'] - minimums['Departure_minute'])
finalDf['PRCP'] = (finalDf['PRCP'] - minimums['PRCP'])/(maximums['PRCP'] - minimums['PRCP'])
finalDf['SNOW'] = (finalDf['SNOW'] - minimums['SNOW'])/(maximums['SNOW'] - minimums['SNOW'])
finalDf['TMAX'] = (finalDf['TMAX'] - minimums['TMAX'])/(maximums['TMAX'] - minimums['TMAX'])
finalDf['TMIN'] = (finalDf['TMIN'] - minimums['TMIN'])/(maximums['TMIN'] - minimums['TMIN'])

# Add airline Later
df['Delay_proba'] = [x[1] for x in model.predict_proba(finalDf[["Airline", "Year", "Month","depAirport", "arrAirport",  "Departure_hour", "Departure_minute", 'PRCP', 'SNOW', 'TMAX', 'TMIN']])]
df["Delay_Prediction"] = model.predict(finalDf[["Airline", "Year", "Month","depAirport", "arrAirport",  "Departure_hour", "Departure_minute", 'PRCP', 'SNOW', 'TMAX', 'TMIN']])

# Save the new dataframe with predictions into a table in Hive
print("Predictions updated")
sparkFinal = spark.createDataFrame(df.drop(["tmax", "tmin", "prcp", "snow"], axis = 1))
#sparkFinal = spark.createDataFrame(finalDf)
spark.sql("DROP TABLE IF EXISTS delayPredictions")
sparkFinal.write.mode("overwrite").saveAsTable("delayPredictions")



