from delta import *
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pathlib import Path
import json
import os
from log import *


TABLE_RULES_SRC = Path("./ingestion_configuration")
WAREHOUSE_DIR = Path("./spark_project/spark-warehouse")

def validate_table(df: DataFrame, rules):
	schema_issues = []
	# check table not empty
	if not df.count() > 0:
		# print("✗ Table is empty")
		schema_issues.append("✗ Table is empty")
	else:
		print("✓ table not empty")

	# non-nullable columns
	for col in rules["columns"]:
		if not col["nullable"] and col["source"] is not None:
			if df.filter(df[col["name"]].isNull()).count() > 0:
				# print(f"✗ Column {col['name']} contains null values")
				schema_issues.append(f"✗ Column {col['name']} contains null values")
	print("✓ non-nullable columns validated")

	# verify expected columns
	expected_cols = set([col["name"] for col in rules["columns"] if col["source"] is not None])
	if not set(df.columns).issuperset(expected_cols):
		# print(f"✗ Columns do not match expected columns: {set(df.columns)} vs {expected_cols}")
		schema_issues.append(f"✗ Columns do not match expected columns: {set(df.columns)} vs {expected_cols}")
	else:
		print("✓ found all columns")

	# verify data type
	for col in rules["columns"]:
		if col["source"] is None:
			continue
		col_type = dict(df.dtypes)[col["name"]]
		if col_type != col["type"]:
			# print(f"✗ Column {col['name']} has incorrect data type: {col_type}")
			schema_issues.append(f"✗ Column {col['name']} has incorrect data type: {col_type}")
	print("✓ data type validated")

	return schema_issues

def filter_table(dt: DeltaTable, rules):
	del_cond = []
	# Note: conditions in config are assumed to be 'keep' conditions for readability, negate for deletion

	# attribute values range
	for col in rules["columns"]:
		if "expr" in col:
			if col["nullable"]:
				del_cond.append(f"(NOT ({col['expr']}) AND {col['name']} IS NOT NULL)") # del if value violates expr if exists
			else:
				del_cond.append(f"(NOT ({col['expr']}) OR {col['name']} IS NULL)") # del if value violates expr or is null

	# inter-attribute relations
	if "row_expr" in rules:
		for row_expr in rules["row_expr"]:
			del_cond.append(f"NOT ({row_expr})")

	# reference records
	for fk in rules["foreign_keys"]:
		del_cond.append(f"{fk} IS NULL")

	# remove rows
	if del_cond:
		del_comm = " OR ".join(del_cond)
		dt.delete(del_comm)

		print(f"Table {rules['name']} cleaned")


if __name__ == "__main__":
	from custom_builder import builder
	spark = configure_spark_with_delta_pip(builder).getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	for rule_file in os.listdir(TABLE_RULES_SRC):
		with open(Path(TABLE_RULES_SRC / rule_file), "r") as f:
			rules = json.load(f)

		table_path = str(WAREHOUSE_DIR / rules['name'])
		df = spark.read.format("delta").load(table_path)

		print(f"\n{'⠛'*80}\nValidating {rules['name']}...")
		with log_step(f"validate_data"):

			if schema_issues := validate_table(df, rules):
				print("\n".join(schema_issues))

			dt = DeltaTable.forPath(spark, table_path)
			filter_table(dt, rules)

			# remove duplicate rows
			if pk_cols := rules["primary_keys"]:
				df = dt.toDF()
				if df.groupBy(pk_cols).count().filter("count > 1").count() > 0: # only rewrite if deplicates exist
					df = df.dropDuplicates(pk_cols)
					df.write.format("delta").mode("overwrite").save(table_path)
