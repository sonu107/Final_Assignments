# Data Loading

# Import required libraries.
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataLoader") \
    .getOrCreate()

# Load dataset from flights.parquet into a PySpark DataFrame
flights_df = spark.read.parquet("/Users/sonushah/Desktop/final_assignments/Flights 1m.parquet")

# Display the schema of the DataFrame to understand its structure
flights_df.printSchema()

flights_df.show()

# Data Preprocessing

# NULL Value Check
# Assess each column for NULL values.

from pyspark.sql.functions import col, when, split

null_counts = {col_name: flights_df.filter(col(col_name).isNull()).count() for col_name in flights_df.columns}
print("NULL counts per column:", null_counts)

# Devise strategies for handling columns with significant NULL values (e.g., removal, imputation).

# For demonstration, let's impute NULL values with the mean for numeric columns and with a default
# value for string columns.

for col_name in flights_df.columns:
    if null_counts[col_name] > 0:
        if flights_df.schema[col_name].dataType == "string":
            default_value = "Unknown"  # Specify the default value for string columns
            flights_df = flights_df.fillna(default_value, subset=[col_name])
        else:
            mean_value = flights_df.select(col_name).agg({col_name: "mean"}).collect()[0][0]
            flights_df = flights_df.fillna(mean_value, subset=[col_name])
            

# Show the DataFrame after handling NULL values
flights_df.show()

# Remove rows with any null values.
flights_df_no_null = flights_df.na.drop()

# Show the DataFrame after removing null values
flights_df_no_null.show()

# Handle records with empty or NULL FL_Date.
flights_df_empty_null = flights_df.filter(col("FL_DATE").isNotNull() | (col("FL_Date") != ""))

flights_df_empty_null.show()

# Split FL_Date into Date and Time columns
flights_df = flights_df.withColumn("Date", split(col("FL_Date"), " ")[0])
flights_df = flights_df.withColumn("Time", split(col("FL_Date"), " ")[1])

# Show the DataFrame after FL_Date handling and splitting.
flights_df.show()

# Handle outliers(e.g., remove rows with Dep_Delay greater than 100).
flights_df_outliers = flights_df.filter(col("DEP_DELAY") < 100)
flights_df_outliers.show()

# Ensure correct data types.
# Convert FL_DATE column to timestamp data type.
flights_df_convert = flights_df.withColumn("FL_DATE", col("FL_DATE").cast("timestamp"))
flights_df_convert.show()

# Renaming columns(e.g., rename Dep_Delay to Departure_Delay).
flights_df_rename = flights_df.withColumnRenamed("Dep_Delay", "Departure_Delay")
flights_df_rename.show()

