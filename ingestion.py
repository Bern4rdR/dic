from typing import Any

import json
from delta import *
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from custom_builder import builder
from log import *


DB_SRC = "data"


def ingest(spark, conf) -> tuple[DataFrame, Any]:
    # 1. Load the configuration and dataframe
    with open(conf) as f:
        config = json.load(f)


    if config["source"].endswith(".parquet"):
        df = spark.read \
            .option("header", True) \
            .option("inferSchema", False) \
            .parquet(config["source"])
    else:
        df = spark.read \
            .option("header", True) \
            .option("inferSchema", False) \
            .csv(config["source"])

    return df, config

def transform(df, config) -> DataFrame:
	# 2. Build the selected/transformed columns
    selected_columns = []

    for column in config["columns"]:
        source = column["source"]
        name = column["name"]
        data_type = column["type"]

        if source is None:
            continue

        if isinstance(source, list):
            if data_type == "timestamp":
                if config["source"] == f"{DB_SRC}/air_quality/hourly_88101_2024.csv":
                    expr = F.to_timestamp(
                        F.concat_ws(" ", *[F.col(c) for c in source]),
                        "yyyy-MM-dd HH:mm"
                    ).alias(name)
                elif config["source"] == f"{DB_SRC}/weather.csv":
                    expr = F.to_timestamp(
                        F.concat_ws(
                            " ",
                            F.concat_ws(
                                "-",
                                F.col(source[0]),
                                F.lpad(F.col(source[1]), 2, "0"),
                                F.lpad(F.col(source[2]), 2, "0")
                            ),
                            F.concat(
                                F.lpad(F.col(source[3]), 2, "0"),
                                F.lit(":00")
                            )
                        ),
                        "yyyy-MM-dd HH:mm"
                    ).alias(name)
            else:
                raise ValueError(
                    f"Multiple source columns are only handled for timestamp, "
                    f"got type={data_type}"
                )

        else:
            expr = F.col(source)

            # Apply configured type
            if data_type == "string":
                expr = expr.cast("string")
            elif data_type == "float":
                expr = expr.cast("float")
            elif data_type == "int":
                expr = expr.cast("int")
            elif data_type == "timestamp":
                expr = F.to_timestamp(expr)

            expr = expr.alias(name)

        selected_columns.append(expr)

    # print(f"Selected columns: {[c._jc.toString() for c in selected_columns]}")
    return df.select(*selected_columns)


if __name__ == "__main__":
	spark = configure_spark_with_delta_pip(builder).getOrCreate()

	print(f'current database: {spark.catalog.currentDatabase()}')
	print(f'spark tables: {spark.catalog.listTables()}')

	for config_file in Path("ingestion_configuration").glob("*.json"):
		df, config = ingest(spark, config_file)
		result_df = transform(df, config)

		result_df.printSchema()
		result_df.show(truncate=False)

		with log_step("write_delta"):
	        # persist delta table
			result_df.write.format("delta").mode("overwrite").saveAsTable(f"{config['name']}")


# df = read_source(spark, config["source"])
# df = transform_columns(df, config["columns"])
# df = add_audit_columns(df, config["audit"])
# validate(df, config)
# write_delta(df, config["target"])

# ---------------------------------------

# from pyspark.sql.types import (
#     StructType, StructField,
#     StringType, IntegerType, DoubleType
# )


# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("amount", DoubleType(), True)
# ])

# df = spark.read \
#     .option("header", "true") \
#     .schema(schema) \
#     .csv("/path/to/input.csv")

# df.write \
#     .format("delta") \
#     .mode("append") \
#     .saveAsTable("my_database.my_table")
