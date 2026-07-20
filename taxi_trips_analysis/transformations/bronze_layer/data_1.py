import dlt
#loading raw data
@dlt.table(
    name="bronze_ubar",
    table_properties={
        "delta.columnMapping.mode": "name"}
)
def bronze_ubar():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
            .option("header", "true")\
            .load("/Volumes/ubar/bronze/bronze_volume/")
    return df