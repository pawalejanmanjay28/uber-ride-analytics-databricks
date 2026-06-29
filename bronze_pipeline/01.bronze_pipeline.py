from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name='bronze_rides',
    comment="Raw uber ride data"
)

def bronze_rides():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.schemaLocation","/Volumes/uber_india/bronze/schema_metadata")
        .load("/Volumes/uber_india/bronze/landing")
        .withColumn("source_file_name",col("_metadata.file_path"))

    )
