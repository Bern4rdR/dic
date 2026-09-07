from delta import *
import pyspark
from pathlib import Path

BASE_DIR = Path("spark_project")

WAREHOUSE_DIR = BASE_DIR / "spark-warehouse"
METASTORE_DIR = BASE_DIR / "metastore_db"

builder = (
    pyspark.sql.SparkSession.builder
    .appName("MyApp")

    # Delta
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    # Persistent table storage
    .config(
        "spark.sql.warehouse.dir",
        str(WAREHOUSE_DIR)
    )

    # Persistent Hive metastore
    .config(
        "javax.jdo.option.ConnectionURL",
        f"jdbc:derby:{METASTORE_DIR};create=true"
    )

	# specify compression codec for linux (arch, btw) compatability
    .config("spark.sql.parquet.compression.codec", "zstd")

    # IMPORTANT
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(f'current database: {spark.catalog.currentDatabase()}')
print(f'spark tables: {spark.catalog.listTables()}')

# table_name = "default.location_lookup"
# if not spark.catalog.tableExists(table_name):
#     # Read CSV, note that we are inferring the schema here, but we will change it to define the schema explicitly
#     df = spark.read \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .csv("data/taxi_zone_lookup.csv")

#     # Write as Delta
#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .saveAsTable(table_name)
# else:
#     print("Delta table already exists — skipping ingestion")

# df = spark.read.table(table_name)

# df.show()

# df = spark.sql("""
#     SELECT *
#     FROM location_lookup
#     WHERE Borough = 'Manhattan'
# """)

# df.show()

from pyspark.sql import functions as F

import json
from pathlib import Path

for config_file in Path("ingestion_configuration").glob("*.json"):

    with open(config_file) as f:
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
                if config["source"] == "data/air_quality/hourly_88101_2024.csv":
                    expr = F.to_timestamp(
                        F.concat_ws(" ", *[F.col(c) for c in source]),
                        "yyyy-MM-dd HH:mm"
                    ).alias(name)
                elif config["source"] == "data/weather.csv":
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

    # 3. Select only the configured columns
    result_df = df.select(*selected_columns)

    result_df.printSchema()
    result_df.show(truncate=False)


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
