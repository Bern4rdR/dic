from delta.pip_utils import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions.builtin import last

import Task_3

import sys
sys.path.append('../')
from custom_builder import builder



def daily_county_demand(spark: SparkSession) -> DataFrame:
	# daily number of trips by county
	return spark.sql("""
		SELECT TO_DATE(pu_datetime) AS trip_date, pu_county, COUNT(*) AS total_trips
	    FROM integrated_taxi_trips
	    WHERE pu_county IS NOT NULL
	    GROUP BY trip_date, pu_county
	    ORDER BY trip_date, pu_county;
	""")

# Corresponds to query_1_4?
def weather_impact_demand(spark: SparkSession) -> DataFrame:
	# impact of weather on taxi trips
	return spark.sql("""
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
	""")

# Corresponds to query_1_5
def week_day_demand(spark: SparkSession) -> DataFrame:
	# number of trips by day of the week
	return spark.sql("""
		SELECT DATE_FORMAT(pu_datetime, 'EEE') AS day, COUNT(*) AS trips
			FROM integrated_taxi_trips
			GROUP BY day
			ORDER BY trips
	""")

def conuty_trip_flow(spark: SparkSession) -> DataFrame:
	# inter-county trip flow
	return spark.sql("""
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
	""")

def needs_recompute(spark: SparkSession, table_name: str, current_delta_ver: int, current_schema_ver: str) -> bool:
	# check delta and schema ver if table needs recompute
	if not spark.catalog.tableExists(table_name):
		return True

	try:
		props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
		props_dict = {row["key"]: row["value"] for row in props}

		last_delta_ver = int(props_dict.get("source_delta_version", -1))
		last_schema_ver = props_dict.get("source_schema_version", "-1")

		return (last_delta_ver < current_delta_ver) or (last_schema_ver != current_schema_ver)
	except:
		print(f"failed to check delta/schema ver for {table_name}")
		return True


def publish_products(spark: SparkSession):
	source_table = "integrated_taxi_trips"
	current_schema_ver = str(Task_3.get_current_schema_version(spark, source_table))
	current_delta_ver = DeltaTable.forName(spark, source_table).history(1).select("version").first()["version"]

	data_products = [
		("daily_county_demand", daily_county_demand, "trip_date"),
		("weather_impact_demand", weather_impact_demand, None),
		("week_day_demand", week_day_demand, None),
		("conuty_trip_flow", conuty_trip_flow, None),
	]

	for table_name, data_product, partition_col in data_products:
		print(f"\n{'⠛'*80}")
		if not needs_recompute(spark, table_name, current_delta_ver, current_schema_ver):
			print(f"skipping {table_name}: already up to date")
			continue

		print(f"(re)computing {table_name}")

		df = data_product(spark)
		writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

		if partition_col: writer = writer.partitionBy(partition_col)

		writer.saveAsTable(table_name)

		spark.sql(f"""
			ALTER TABLE {table_name} SET TBLPROPERTIES (
				'source_delta_version' = '{current_delta_ver}',
				'source_schema_version' = '{current_schema_ver}'
			)
		""")

		print(f"Product {table_name} published")


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()
	publish_products(spark)
