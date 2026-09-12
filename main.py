import numpy as np
import pandas as pd
from pathlib import Path
from log import *
from delta import *

from custom_builder import builder
from ingestion import ingest, transform
from validation import validate_table, filter_table


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()

	print(f'current database: {spark.catalog.currentDatabase()}')
	print(f'spark tables: {spark.catalog.listTables()}')

	for config_file in Path("ingestion_configuration").glob("*.json"):
		# Ingest
		df, config = ingest(spark, config_file)

		# Transform
		result_df = transform(df, config)

		# Validate
		print(f"\n{'⠛'*80}\nValidating {config['name']}...")
		with log_step(f"validate_data") as info:
			validate_table(result_df, config)
			filtered_df = filter_table(result_df, config["name"])
			row_diff = result_df.count() - filtered_df.count()
			# info["removed_rows"] = row_diff
			print(f"{row_diff} rows removed ({filtered_df.count()} remaining)")

		filtered_df.printSchema()
		filtered_df.show(truncate=False)

		with log_step("write_delta"):
	        # persist delta table
			filtered_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{config['name']}")
