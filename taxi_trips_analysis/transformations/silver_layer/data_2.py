import dlt
from pyspark.sql.functions import *

#creating silver table
@dlt.table(
    name="silver_ubar"
)
def silver_ubar():

    df = spark.readStream.table("bronze_ubar")

    # Rename columns because space not allowed in delta
    df = (
        df.withColumnRenamed("Trip ID", "trip_id")
          .withColumnRenamed("Pickup time", "pickup_time")
          .withColumnRenamed("Dropoff time", "dropoff_time")
          .withColumnRenamed("Pickup location", "pickup_location")
          .withColumnRenamed("Dropoff location", "dropoff_location")
          .withColumnRenamed("Fare amount", "fare_amount")
          .withColumnRenamed("Distance (km)", "distance_km")
          .withColumnRenamed("Passenger count", "passenger_count")
    )

    # Convert string to timestamp
    df = (
        df.withColumn("pickup_time", to_timestamp("pickup_time"))
          .withColumn("dropoff_time", to_timestamp("dropoff_time"))
    )

    # Remove duplicate Trip IDs
    df = df.dropDuplicates(["trip_id"])

    # Data quality checks
    df = (
        df.filter(col("fare_amount") > 0)
          .filter(col("distance_km") > 0)
          .filter(col("passenger_count") > 0)
          .filter(col("trip_id").isNotNull())
          .filter(col("pickup_time").isNotNull())
          .filter(col("pickup_location").isNotNull())
    )

    # Create trip duration in minutes
    df = df.withColumn(
        "trip_duration",
        (unix_timestamp("dropoff_time") - unix_timestamp("pickup_time")) / 60
    )

    # Add date/time features
    df = (
        df.withColumn("pickup_date", to_date("pickup_time"))
          .withColumn("pickup_hour", hour("pickup_time"))
          .withColumn("pickup_day", date_format("pickup_time", "EEEE"))
    )

    return df