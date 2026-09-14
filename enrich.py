from delta import *
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pathlib import Path

from pyspark.sql.session import SparkSession
from custom_builder import builder


BASE_DIR = Path("spark_project")
WAREHOUSE_DIR = BASE_DIR / "spark-warehouse"
METASTORE_DIR = BASE_DIR / "metastore_db"


def load_dfs(spark: SparkSession):
	trips_df = spark.read.format("delta").load(str(WAREHOUSE_DIR / "taxi_trips"))
	zone_df = spark.read.format("delta").load(str(WAREHOUSE_DIR / "taxi_zone_lookup"))
	aq_df = spark.read.format("delta").load(str(WAREHOUSE_DIR / "air_quality"))
	weather_df = spark.read.format("delta").load(str(WAREHOUSE_DIR / "weather"))

	pu_zone = zone_df.select(
		F.col("location_id").alias("pu_location_id"),
		F.col("zone").alias("pu_zone"),
		F.col("county").alias("pu_county"),
	)
	do_zone = zone_df.select(
		F.col("location_id").alias("do_location_id"),
		F.col("zone").alias("do_zone"),
		F.col("county").alias("do_county"),
	)

	return trips_df, pu_zone, do_zone, aq_df, weather_df


def enrich(trips_df: DataFrame, pu_zone: DataFrame, do_zone: DataFrame, aq_df: DataFrame, weather_df: DataFrame):
	return (
		trips_df.alias("t")

		# pu_location_id -> county, zone
		.join(
			pu_zone.alias("pu"),
			F.col("t.pu_location_id") == F.col("pu.pu_location_id"),
			"left",
		)

		# do_location_id -> county, zone
		.join(
			do_zone.alias("do"),
			F.col("t.do_location_id") == F.col("do.do_location_id"),
			"left",
		)

		# air quality at pu_datetime
		.join(
			aq_df.alias("aq"),
			(F.date_trunc("hour", F.col("t.pu_datetime")) == F.col("aq.datetime"))
			& (F.col("pu.pu_county") == F.col("aq.county")),
			"left",
		)
		# weather conditions at pu_datetime
		.join(
			weather_df.alias("w"),
			F.date_trunc("hour", F.col("t.pu_datetime")) == F.col("w.datetime")
		)
		.drop(
			# taxi_trips
			F.col("t.pu_location_id"),
			F.col("t.do_location_id"),
			# taxi_zone_lookup
			F.col("pu.pu_location_id"),
			F.col("do.do_location_id"),
			# air_quality
			F.col("aq.datetime"),
			F.col("aq.county"),
			# weather
			F.col("w.datetime")
		)
	)

if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()

	trips_df, pu_zone, do_zone, aq_df, weather_df = load_dfs(spark)

	enriched_df = enrich(trips_df, pu_zone, do_zone, aq_df, weather_df)

	enriched_df.printSchema()
	enriched_df.show()
	enriched_df.write.format("delta").mode("overwrite").saveAsTable("integrated_taxi_trips")
