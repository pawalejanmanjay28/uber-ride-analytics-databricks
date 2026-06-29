
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum
)


@dp.materialized_view(
    name="gold_daily_summary",
    comment="Daily Rides Business Summary"
)
def gold_daily_summary():

    silver_df = spark.read.table(
        "uber_india.silver.silver_rides"
    )

    gold_df = (
        silver_df
        .groupBy("trip_date")
        .agg(
            count("ride_id").alias("total_rides"),
            sum("distance_km").alias("total_distance"),
            sum("fare_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("driver_rating").alias("average_driver_rating"),
            avg("customer_rating").alias("average_customer_rating")
        )
        .orderBy("trip_date")
    )

    return gold_df



@dp.materialized_view(
    name="gold_city_summary",
    comment="City Ride Business Summary"
)
def gold_city_summary():

    silver_df = spark.read.table(
        "uber_india.silver.silver_rides"
    )

    gold_df = (
        silver_df
        .groupBy("pickup_city")
        .agg(
            count("ride_id").alias("total_rides"),
            sum("fare_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("distance_km").alias("average_trip_distance")
        )
        .orderBy(col("total_revenue").desc())
    )

    return gold_df



@dp.materialized_view(
    name="gold_vehicle_summary",
    comment="Vehicle Ride Business Summary"
)
def gold_vehicle_summary():

    silver_df = spark.read.table(
        "uber_india.silver.silver_rides"
    )

    gold_df = (
        silver_df
        .groupBy("vehicle_type")
        .agg(
            count("ride_id").alias("total_rides"),
            sum("fare_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("distance_km").alias("average_trip_distance"),
            avg("driver_rating").alias("average_driver_rating"),
            avg("customer_rating").alias("average_customer_rating")
        )
        .orderBy(col("total_revenue").desc())
    )

    return gold_df



@dp.materialized_view(
    name="gold_payment_summary",
    comment="Payment Business Summary"
)
def gold_payment_summary():

    silver_df = spark.read.table(
        "uber_india.silver.silver_rides"
    )

    gold_df = (
        silver_df
        .groupBy("payment_mode")
        .agg(
            count("ride_id").alias("total_rides"),
            sum("fare_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("distance_km").alias("average_trip_distance")
        )
        .orderBy(col("total_revenue").desc())
    )

    return gold_df



@dp.materialized_view(
    name="gold_driver_summary",
    comment="Driver Business Summary"
)
def gold_driver_summary():

    silver_df = spark.read.table(
        "uber_india.silver.silver_rides"
    )

    gold_df = (
        silver_df
        .groupBy("driver_id")
        .agg(
            count("ride_id").alias("total_rides"),
            sum("fare_amount").alias("total_revenue"),
            avg("driver_rating").alias("average_driver_rating"),
            avg("distance_km").alias("average_trip_distance"),
            avg("fare_amount").alias("average_fare")
        )
        .orderBy(col("total_rides").desc())
    )

    return gold_df