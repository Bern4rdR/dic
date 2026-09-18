from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession

import sys
sys.path.append('../')
from custom_builder import builder


def _add_metadata(df: DataFrame, source: str, schema_ver: str) -> DataFrame:
	now = F.current_timestamp()
	return (
		df.withColumn("_metadata_source_table", F.lit(source))
		.withColumn("_metadata_schema_version", F.lit(schema_ver))
		.withColumn("_metadata_created", now)
		.withColumn("_metadata_updated", now)
	)


def daily_county_demand(spark: SparkSession) -> DataFrame:
	# daily number of trips by county
	return _add_metadata(spark.sql("""
		SELECT TO_DATE(pu_datetime) AS trip_date, pu_county, COUNT(*) AS total_trips
	    FROM integrated_taxi_trips
	    WHERE pu_county IS NOT NULL
	    GROUP BY trip_date, pu_county
	    ORDER BY trip_date, pu_county;
	"""), "integrated_taxi_trips", "1.1.0")

# Corresponds to query_2_4 in Queries.py?, TODO: Should do query_2_4() function call
def weather_impact_demand(spark: SparkSession) -> DataFrame:
	# impact of weather on taxi trips
	return _add_metadata(spark.sql("""
		WITH
		    weather_cat AS (
		        SELECT pu_county, DATE_TRUNC('hour', pu_datetime) AS hour,
		            CASE
		                WHEN prcp > 3 THEN 'rain'
		                WHEN temp > 25 THEN 'heatwave'
		                WHEN temp < 0 THEN 'cold'
		                WHEN rhum > 65 THEN 'humid'
		                WHEN rhum < 25 THEN 'dry'
		                WHEN wspd > 8 THEN 'stormy'
		                ELSE 'moderate'
		            END AS weather_cond
		        FROM integrated_taxi_trips
		        WHERE pu_county IS NOT NULL
		    ),
		    demand AS (
		        SELECT weather_cond, pu_county, COUNT(*) AS trips, COUNT(DISTINCT hour) AS weather_hours
		        FROM weather_cat
		        GROUP BY weather_cond, pu_county
		    )
		SELECT weather_cond, pu_county, trips, weather_hours, ROUND(CAST(trips AS DOUBLE) / NULLIF(weather_hours, 0), 2) AS trips_per_hour
		    FROM demand
		    ORDER BY pu_county, trips_per_hour
	"""), "integrated_taxi_trips", "1.0.0")

# Corresponds to query_2_5 in Queries.py, TODO: Should do query_2_5() function call
def week_day_demand(spark: SparkSession) -> DataFrame:
	# number of trips by day of the week
	return _add_metadata(spark.sql("""
		SELECT DATE_FORMAT(pu_datetime, 'EEE') AS day, COUNT(*) AS trips
			FROM integrated_taxi_trips
			GROUP BY day
			ORDER BY trips
	"""), "integrated_taxi_trips", "1.0.0")

def conuty_trip_flow(spark: SparkSession) -> DataFrame:
	# inter-county trip flow
	return _add_metadata(spark.sql("""
		WITH
		    trips_out AS (
		        SELECT pu_county, COUNT(do_county) AS exits
			        FROM integrated_taxi_trips
			        	WHERE pu_county != do_county
			        GROUP BY pu_county
		    ),
		    trips_in AS (
		        SELECT do_county, COUNT(pu_county) AS entries
			        FROM integrated_taxi_trips
			        	WHERE pu_county != do_county
			        GROUP BY do_county
		    )
		SELECT COALESCE(pu_county, do_county) AS county, COALESCE(exits, 0) AS exits, COALESCE(entries, 0) AS entries, (exits - entries) AS net
		    FROM trips_out
		    FULL OUTER JOIN trips_in
		        ON pu_county = do_county
			ORDER BY net
	"""), "integrated_taxi_trips", "1.0.0")


def publish_products(spark: SparkSession):
	data_products = [
		("daily_county_demand", daily_county_demand(spark), "trip_date"),
		("weather_impact_demand", weather_impact_demand(spark), None),
		("week_day_demand", week_day_demand(spark), None),
		("conuty_trip_flow", conuty_trip_flow(spark), None),
	]

	for table_name, df, partition_col in data_products:
		writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

		if partition_col: writer = writer.partitionBy(partition_col)

		writer.saveAsTable(table_name)

		print(f"Product {table_name} published")


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()
	publish_products(spark)
