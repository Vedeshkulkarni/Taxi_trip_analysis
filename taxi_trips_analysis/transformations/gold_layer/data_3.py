import dlt
from pyspark.sql.functions import *
#shows which hours are peak hours
@dlt.table(
    name="gold_peak_hours"
)
def gold_peak_hours():
    df = spark.readStream.table("silver_ubar")
#transformation
    df = (df.groupBy("pickup_hour").agg(count("trip_id").alias("total_trips"),sum("fare_amount").alias("revenue")).orderBy(col("total_trips").desc()))
    return df

#calculate dily revenue
@dlt.table(
    name="gold_daily_revenue"
)
def gold_daily_revenue():
    df = spark.readStream.table("silver_ubar")
    df = (df.groupBy("pickup_date").agg(count("trip_id").alias("trips"),sum("fare_amount").alias("revenue")).orderBy("pickup_date"))
    return df

#calcultate which location is having more trips

@dlt.table(
    name="gold_pickup_location"
)
def gold_pickup_location():
    df = spark.readStream.table("silver_ubar")
    df = (df.groupBy("pickup_location").agg(count("trip_id").alias("trips"),sum("fare_amount").alias("revenue")).orderBy(col("trips").desc()))
    return df

#which destination is more popular

@dlt.table(
    name="gold_dropoff_location"
)
def gold_dropoff_location():
    df = spark.readStream.table("silver_ubar")
    df = (df.groupBy("dropoff_location").agg(count("trip_id").alias("trips")).orderBy(col("trips").desc()))
    return df

#calculate which perticular route is more popular
@dlt.table(
    name="gold_route_analysis"
)
def gold_route_analysis():
    df = spark.readStream.table("silver_ubar")
    df = (df.groupBy("pickup_location", "dropoff_location").agg(count("trip_id").alias("trips"),sum("fare_amount").alias("revenue")).orderBy(col("trips").desc()))
    return df



@dlt.table(
    name="gold_passenger_analysis"
)
def gold_passenger_analysis():
    df = spark.readStream.table("silver_ubar")
    df = (df.groupBy("passenger_count").agg(count("trip_id").alias("trips")).orderBy(col("trips").desc()))
    return df


# calculate distance range
@dlt.table(
    name="gold_distance_analysis"
)
def gold_distance_analysis():
    df = spark.readStream.table("silver_ubar")
    df = df.withColumn("distance_range",
                       when(col("distance_km") <= 5, "0-5 km")
                       .when(col("distance_km") <= 10, "6-10 km")
                       .when(col("distance_km") <= 20, "11-20 km")
                       .otherwise("20+ km"))
    df = (df.groupBy("distance_range").agg(count("trip_id").alias("trips")).orderBy("distance_range"))
    return df

#calculate fare range

@dlt.table(
    name="gold_fare_analysis"
)
def gold_fare_analysis():
    df = spark.readStream.table("silver_ubar")
    df = df.withColumn("fare_range",
                       when(col("fare_amount") <= 20, "₹0-20")
                       .when(col("fare_amount") <= 50, "₹21-50")
                       .when(col("fare_amount") <= 100, "₹51-100").
                       when(col("fare_amount") <= 200, "₹101-200")
                       .otherwise("₹200+"))
    df = (df.groupBy("fare_range").agg(count("trip_id").alias("trips")).orderBy("fare_range"))
    return df


#calculte trip duration

@dlt.table(
    name="gold_trip_duration"
)
def gold_trip_duration():
    df = spark.readStream.table("silver_ubar")
    df = df.withColumn("duration_range",
                       when(col("trip_duration") <= 10, "0-10 min")
                       .when(col("trip_duration") <= 20, "11-20 min")
                       .when(col("trip_duration") <= 30, "21-30 min")
                       .when(col("trip_duration") <= 60, "31-60 min")
                       .otherwise("60+ min"))
    df = (df.groupBy("duration_range").agg(count("trip_id").alias("trips")).orderBy("duration_range"))
    return df

