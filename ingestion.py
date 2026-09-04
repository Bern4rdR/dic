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

table_name = "default.location_lookup"
if not spark.catalog.tableExists(table_name):
    # Read CSV, note that we are inferring the schema here, but we will change it to define the schema explicitly
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv")

    # Write as Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
else:
    print("Delta table already exists — skipping ingestion")

df = spark.read.table(table_name)

df.show()

df = spark.sql("""
    SELECT *
    FROM location_lookup
    WHERE Borough = 'Manhattan'
""")

df.show()

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
