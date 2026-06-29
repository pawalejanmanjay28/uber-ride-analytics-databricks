from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    to_date,
    to_timestamp,


)
@dp.table(
    name="silver_rides",
    comment="Cleaned Uber Ride Data"
)
def silver_rides():
   bronze_df=spark.readStream.table(
       "uber_india.bronze.bronze_rides"
       )
   
   silver_df=(
       bronze_df
       .withColumn("load_timestamp",to_timestamp("load_timestamp"))
       .withWatermark("load_timestamp","1 day")
       .dropDuplicates(["ride_id"])
       .dropna(subset=["ride_id","driver_id","customer_id"])
       .withColumn("ride_id",col('ride_id').cast("int"))
       .withColumn("trip_date",to_date("trip_date"))
       .withColumn("pickup_timestamp",to_timestamp("pickup_timestamp"))
       .withColumn("dropoff_timestamp",to_timestamp("dropoff_timestamp"))
       .withColumn("distance_km",col("distance_km").cast("double"))
       .withColumn("fare_amount",col("fare_amount").cast("double"))
       .withColumn("surge_multiplier",col("surge_multiplier").cast("double"))
       .withColumn("driver_rating",col("driver_rating").cast("double"))
       .withColumn("customer_rating",col("customer_rating").cast("double"))
       .withColumn("luggage_count",col("luggage_count").cast("int"))
       .withColumn("pickup_city",upper(trim(col("pickup_city"))))
       .withColumn("drop_city",upper(trim(col("drop_city"))))
       .withColumn("vehicle_type",upper(trim(col("vehicle_type"))))
       .withColumn("payment_mode",upper(trim(col("payment_mode"))))
       .withColumn("trip_status",upper(trim(col("trip_status"))))
       .withColumn("fuel_type",upper(trim(col("fuel_type"))))
       .withColumn("booking_channel",upper(trim(col("booking_channel"))))
       .withColumn("processed_timestamp",current_timestamp())
       .filter(col("fare_amount")>0)
       .filter(col("distance_km")>0)
       .filter(col("driver_rating").between(1,5))
       .filter(col("customer_rating").between(1,5))                 
   )
   return silver_df