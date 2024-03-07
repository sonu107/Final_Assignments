# Calculating Metrics & Trends.

# Import required libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, corr, month

# Create a Spark session
spark = SparkSession.builder \
    .appName("FlightAnalysis") \
    .getOrCreate()

# Load the flight data into a DataFrame.
flights_df = spark.read.parquet("/Users/sonushah/Desktop/final_assignments/Flights 1m.parquet")

# Displaying the dataframe.
flights_df.show(5)

# Calculate average departure and arrival delay for all flights.
avg_delays = flights_df.select(avg(col("DEP_DELAY")).alias("AvgDepartureDelay"),
                               avg(col("ARR_DELAY")).alias("AvgArrivalDelay")).collect()[0]
print("Average Departure Delay:", avg_delays["AvgDepartureDelay"])
print("Average Arrival Delay:", avg_delays["AvgArrivalDelay"])

# Identify the longest flight by air time, including its distance and delays.
longest_flight = flights_df.select(max(col("AIR_TIME")).alias("LongestAirTime")).collect()[0]
longest_flight_info = flights_df.filter(col("AIR_TIME") == longest_flight["LongestAirTime"]).first()
print("Longest Flight Air Time:", longest_flight_info["AIR_TIME"])
print("Longest Flight Distance:", longest_flight_info["DISTANCE"])
print("Longest Flight Departure Delay:", longest_flight_info["DEP_DELAY"])
print("Longest Flight Arrival Delay:", longest_flight_info["ARR_DELAY"])

# Determine the time of day with the least average departure delay.
min_dep_delay_time = flights_df.groupBy("DEP_TIME").agg(avg("DEP_DELAY").alias("AvgDepDelay")).orderBy("AvgDepDelay").first()
print("Time of Day with Least Average Departure Delay:", min_dep_delay_time["DEP_TIME"])

# Evaluate the correlation between departure delay and arrival delay, alongside air time and distance impact.
correlation = flights_df.select(corr("DEP_DELAY", "ARR_DELAY").alias("DepArrDelayCorrelation"),
                                corr("AIR_TIME", "DISTANCE").alias("AirTimeDistanceCorrelation")).collect()[0]
print("Departure Delay vs Arrival Delay Correlation:", correlation["DepArrDelayCorrelation"])
print("Air Time vs Distance Correlation:", correlation["AirTimeDistanceCorrelation"])

# Examine how average departure and arrival delays vary by month.
avg_delays_by_month = flights_df.groupBy(month("FL_DATE").alias("Month")) \
    .agg(avg("DEP_DELAY").alias("AvgDepartureDelay"), avg("ARR_DELAY").alias("AvgArrivalDelay")) \
    .orderBy("Month").collect()

print("Average Departure and Arrival Delays by Month:")
for row in avg_delays_by_month:
    print("Month:", row["Month"], "| Avg Departure Delay:", row["AvgDepartureDelay"], "| Avg Arrival Delay:", row["AvgArrivalDelay"])

# Categorization & Data Preparation.

# Categorize flights based on delay severity with defined thresholds.

from pyspark.sql.functions import when

# Define delay severity thresholds.
thresholds = {'Minor': 15, 'Moderate': 30, 'Major': 60}

# Categorize flights based on delay severity
categorized_df = flights_df.withColumn("DelaySeverity",
                               when(flights_df["DEP_DELAY"] <= thresholds['Minor'], "Minor")
                               .when(flights_df["DEP_DELAY"] <= thresholds['Moderate'], "Moderate")
                               .otherwise("Major"))
categorized_df.show()

# Analyze if longer flights are more prone to longer delays than shorter ones.

# Calculate average delay for shorter and longer flights.
average_delay_by_distance = flights_df.groupBy("DISTANCE") \
    .agg(avg("DEP_DELAY").alias("AvgDepartureDelay")) \
    .orderBy("DISTANCE")

average_delay_by_distance.show()

# Observe how departures and arrivals vary hourly, identifying peak hours.

# Extract hour from departure and arrival timestamps
flights_df = flights_df.withColumn("DepartureHour", col("DEP_TIME").cast("integer"))
flights_df = flights_df.withColumn("ArrivalHour", col("ARR_TIME").cast("integer"))

# Calculate average departures and arrivals by hour
avg_departures_by_hour = flights_df.groupBy("DepartureHour").avg("DEP_DELAY").orderBy("DepartureHour")
avg_arrivals_by_hour = flights_df.groupBy("ArrivalHour").avg("ARR_DELAY").orderBy("ArrivalHour")

# Identify peak hours based on the highest average delay
peak_departure_hour = avg_departures_by_hour.orderBy(col("avg(DEP_DELAY)").desc()).first()["DepartureHour"]
peak_arrival_hour = avg_arrivals_by_hour.orderBy(col("avg(ARR_DELAY)").desc()).first()["ArrivalHour"]

print("Peak Departure Hour:", peak_departure_hour)
print("Peak Arrival Hour:", peak_arrival_hour)

# Prepare a dataset for a machine learning model to predict arrival delay.

# Select relevant features and target variable
ml_dataset = flights_df.select("DISTANCE", "AIR_TIME", "DEP_DELAY", "ARR_DELAY").show()

# Identify the extremes in flight operations for both departure and arrival delays and write these analyses
#into an Excel file with different sheets.

import pandas as pd

extreme_departure_delay = flights_df.select(max("DEP_DELAY").alias("MaxDepartureDelay"), min("DEP_DELAY").alias("MinDepartureDelay"))
extreme_arrival_delay = flights_df.select(max("ARR_DELAY").alias("MaxArrivalDelay"), min("ARR_DELAY").alias("MinArrivalDelay"))

# Write analyses into an Excel file with different sheets
with pd.ExcelWriter('flight_analysis.xlsx') as writer:
    extreme_departure_delay.toPandas().to_excel(writer, sheet_name='Extreme Departure Delay', index=False)
    extreme_arrival_delay.toPandas().to_excel(writer, sheet_name='Extreme Arrival Delay', index=False)

    