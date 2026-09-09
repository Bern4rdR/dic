from delta import *
from pyarrow import NullArray
import pyspark
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pathlib import Path
import json
import os


TABLE_RULES_SRC = Path("./ingestion_configuration")
WAREHOUSE_DIR = Path("./spark_project/spark-warehouse")

def validate_table(df, rules):
	# check table not empty
	if not df.count() > 0:
		# print("✗ Table is empty")
		raise Exception("✗ Table is empty")
	else:
		print("✓ table not empty")

	# non-nullable columns
	for col in rules["columns"]:
		if not col["nullable"] and col["source"] is not None:
			if df.filter(df[col["name"]].isNull()).count() > 0:
				# print(f"✗ Column {col['name']} contains null values")
				raise Exception(f"✗ Column {col['name']} contains null values")
	print("✓ non-nullable columns validated")

	# verify expected columns
	expected_cols = set([col["name"] for col in rules["columns"] if col["source"] is not None])
	if set(df.columns) != expected_cols:
		# print(f"✗ Columns do not match expected columns: {set(df.columns)} vs {expected_cols}")
		raise Exception("✗ Columns do not match expected columns: {set(df.columns)} vs {expected_cols}")
	else:
		print("✓ found all columns")

	# verify data type
	for col in rules["columns"]:
		if col["source"] is None:
			continue
		col_type = dict(df.dtypes)[col["name"]]
		if col_type != col["type"]:
			# print(f"✗ Column {col['name']} has incorrect data type: {col_type}")
			raise Exception(f"✗ Column {col['name']} has incorrect data type: {col_type}")
	print("✓ data type validated")


def filter_table(df, table_name) -> DataFrame:
	valid_counties = ["Bronx", "Queens", "Manhattan", "Staten Island", "Brooklyn"]

	# verify data range (real date / non-negative number / etc)
	match table_name:
		case "air_quality":
			return df.filter(F.col("county").isin(valid_counties))

		case "taxi_trips":
			return df.filter(
                F.col("pu_datetime").isNotNull()
                & F.col("do_datetime").isNotNull()
                & (F.col("pu_datetime") <= F.col("do_datetime"))
            )

		case "taxi_zone_lookup":
			return df.filter(F.col("county").isin(valid_counties))

		case "weather":
			return df \
				.filter(F.col("rhum").between(0, 100) | F.col("rhum").isNull()) \
				.filter(F.col("cldc").between(0, 100) | F.col("cldc").isNull()) \
				.filter(F.col("wdir").between(0, 360) | F.col("wdir").isNull()) \
				.filter((F.col("wspd") >= 0) | F.col("wspd").isNull()) \
				.filter((F.col("prcp") >= 0) | F.col("prcp").isNull()) \
				.filter((F.col("snwd") >= 0) | F.col("snwd").isNull())

		case _:
			raise Exception(f"✗ Unknown table: '{table_name}'")


if __name__ == "__main__":
	builder = (
	    SparkSession.builder
		    .appName("MyApp")

		    .config(
		        "spark.sql.extensions",
		        "io.delta.sql.DeltaSparkSessionExtension"
		    )
		    .config(
		        "spark.sql.catalog.spark_catalog",
		        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
		    )
			.config("spark.sql.parquet.compression.codec", "zstd")
	)
	spark = configure_spark_with_delta_pip(builder).getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	for rule_file in os.listdir(TABLE_RULES_SRC):
		with open(Path(TABLE_RULES_SRC / rule_file), "r") as f:
			rules = json.load(f)
			df = spark.read.format("delta").load(str(WAREHOUSE_DIR / rules['name']))

			print(f"\n{'⠛'*80}\nValidating {rules['name']}...")
			validate_table(df, rules)
			filtered_df = filter_table(df, rules["name"])

			row_diff = df.count() - filtered_df.count()
			print(f"{row_diff} rows removed ({filtered_df.count()} remaining)")

			# replace existing delta table with filtered data
			filtered_df.write \
				.format("delta") \
				.mode("overwrite") \
				.option("overwriteSchema", "true") \
				.save(f"{WAREHOUSE_DIR}/{rules['name']}")
