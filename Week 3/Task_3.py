import hashlib
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)


SCHEMA_REGISTRY = "schema_versions"


def initialize_registry(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_REGISTRY} (
            table_name STRING,
            schema_version LONG,
            delta_version LONG,
            schema_hash STRING,
            schema_json STRING,
            created_at TIMESTAMP
        )
        USING DELTA
    """)

import hashlib


def register_schema_version(
    spark,
    table_name: str,
):
    """
    Register the current schema of a Delta table if it differs
    from the latest registered schema.
    """

    # Current Delta version
    current_delta_version = (
        spark.sql(f"DESCRIBE HISTORY `{table_name}`")
        .select("version")
        .orderBy(F.col("version").desc())
        .limit(1)
        .collect()[0]["version"]
    )

    print(f"Current Delta version for {table_name}: {current_delta_version}")

    # Current schema
    schema = spark.table(table_name).schema
    schema_json = schema.json()

    print(f"Current schema for {table_name}: {schema_json}")

    schema_hash = hashlib.sha256(
        schema_json.encode("utf-8")
    ).hexdigest()

    # Latest registered schema
    latest = (
        spark.table(SCHEMA_REGISTRY)
        .where(F.col("table_name") == table_name)
        .orderBy(F.col("schema_version").desc())
        .limit(1)
        .collect()
    )

    if latest and latest[0]["schema_hash"] == schema_hash:
        # Schema hasn't changed.
        return latest[0]["schema_version"]

    # First schema = version 0
    if not latest:
        new_schema_version = 0
    else:
        new_schema_version = latest[0]["schema_version"] + 1

    row = [(
        table_name,
        new_schema_version,
        current_delta_version,
        schema_hash,
        schema_json,
    )]

    df = (
        spark.createDataFrame(
            row,
            """
            table_name STRING,
            schema_version LONG,
            delta_version LONG,
            schema_hash STRING,
            schema_json STRING
            """
        )
        .withColumn("created_at", F.current_timestamp())
    )

    df.write.format("delta").mode("append").saveAsTable(
        SCHEMA_REGISTRY
    )

    return new_schema_version

def get_current_schema_version(spark, table_name: str):
    """
    Get the current schema version of a Delta table.
    """

    initialize_registry(spark)

    register_schema_version(spark, table_name)

    current_schema_version = (
        spark.table(SCHEMA_REGISTRY)
        .filter(F.col("table_name") == table_name)
        .select("schema_version")
        .orderBy(F.col("schema_version").desc())
        .limit(1)
        .collect()
    )

    if not current_schema_version:
        return None

    return current_schema_version[0]["schema_version"]