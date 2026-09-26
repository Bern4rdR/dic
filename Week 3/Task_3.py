from datetime import datetime
import hashlib
import json
import sys

from pathlib import Path
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

import hashlib


SRC_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SRC_DIR))
from validation import validate_table, filter_table

from log import log_step

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

def register_schema_version(spark, table_name: str):
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

def initialize_pipeline_monitor_table(spark):
    # Initialize the pipeline monitor table if it doesn't exist.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS pipeline_monitor (
            execution_start_time TIMESTAMP,
            execution_end_time TIMESTAMP,
            processed_records LONG,
            inserted_records LONG,
            rejected_records LONG,
            validation_failures LONG,
            table_name STRING,
            schema_version LONG,
            delta_version LONG
        )
        USING DELTA
    """)

def update_pipeline_execution(spark: SparkSession, new_df, table_name: str):
    """
    Perform update pipeline execution and update the pipeline monitor table with the latest execution details.
    Args:
        new_df: The new DataFrame to be merged into the target Delta table. Retrieved from csv or parquet file.
    """
    processed_records = new_df.count()

    # ------------------------------------------
    # ------ Merge operation setup ------
    # ------------------------------------------

    target = DeltaTable.forName(spark, table_name)
    target_df = spark.table(table_name)

    # Only compare columns that currently exist in the target
    compare_cols = target_df.columns

    # Null-safe equality for every column
    condition = " AND ".join(
        [f"t.`{c}` <=> s.`{c}`" for c in compare_cols]
    )

    # Get the current timestamp for the start of the pipeline execution so that it is easily added to the pipeline monitor table. F.current_timestamp() is not what we want here because it will be evaluated at the time of writing to the table, not at the time of execution.
    pipeline_start_time = datetime.now()

    # ------------------------------------------
    # ------------- Validation -----------------
    # ------------------------------------------

    base_config = SRC_DIR.parent / "ingestion_configuration" / f"{table_name}.json"
    update_config = SRC_DIR.parent / "ingestion_update_configuration" / f"{table_name}.json"

	# load rules from config
    rules = {}
    if update_config.exists():
    	with open(update_config, "r") as f:
            rules = json.load(f)

            if base_config.exists():
                with open(base_config, "r") as bf:
                    base_rules = json.load(bf)

                # get table rules from base config
                for rule_key in ["primary_keys", "foreign_keys", "row_expr"]:
                    if rule_key in base_rules and rule_key not in rules:
                        rules[rule_key] = base_rules[rule_key]

                # get attribute rules from base config
                for attr in rules["columns"]:
                    base_attr = base_rules["columns"][attr]
                    if "expr" in base_attr and "expr" not in attr:
                        attr["expr"] = base_attr["expr"]

    with log_step(f"Validating data for {table_name}", info={"rules": rules}):
        # validate data
        if rules:
            schema_issues = validate_table(new_df, rules)
            if schema_issues:
                print(f"[{table_name}] warnings:\n  " + "\n  ".join(schema_issues))

    # ------------------------------------------
    # ------ Merge operation ------
    # ------------------------------------------

    ( # Merge the new DataFrame into the target Delta table, only inserts non-duplicate rows
        target.alias("t")
        .merge(
            new_df.alias("s"),
            condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    pipeline_end_time = datetime.now()

    # ------------------------------------------
    # ----------- Pipeline monitoring ----------
    # ------------------------------------------

    with log_step(f"Pipeline monitoring for {table_name}"):

        initialize_pipeline_monitor_table(spark)

        # get the latest delta version noted in pipeline_monitor table for this table_name
        latest_delta_version = (
            spark.table("pipeline_monitor")
            .filter(F.col("table_name") == table_name)
            .select("delta_version")
            .orderBy(F.col("delta_version").desc())
            .limit(1)
            .collect()
        )
        # if there is no latest delta version, set it to -1
        if not latest_delta_version:
            latest_delta_version = [{"delta_version": -1}]

        last_logged_version = latest_delta_version[0]["delta_version"]
        current_target_version = target.history(1).select("version").first()["version"]
        # only update the pipeline monitor table if the delta version has changed for the target table (i.e something has changed)
        if current_target_version > last_logged_version:
            print(f"Did update to Delta table {table_name}")

            # Update the pipeline monitor table with the latest execution details.

            current_schema_version = get_current_schema_version(spark, table_name)

            if current_schema_version is None:
                raise ValueError(f"No schema version found for table {table_name}")

            metrics = target.history(1).select("operationMetrics").first()

            # get numTargetRowsInserted from operationMetrics, metrics is a Row object, so we need to access the dictionary inside it
            metrics = metrics.asDict()["operationMetrics"]

            print(f"Operation metrics: {metrics}")

            inserted_rows = int(metrics.get("numTargetRowsInserted", 0))
            rejected_rows = processed_records - inserted_rows

            # count rows that failed to validate TODO: does not appear to be correct, the merge should only try to insert already validated rows so the delta log history won't show anything
            history_df = target.history().filter(F.col("version") > last_logged_version).collect()
            validation_failures = sum([int(commit["operationMetrics"]["numTargetRowsInserted"]) for commit in history_df if commit["operation"] == "DELETE"])

            row = [(
                pipeline_start_time,
                pipeline_end_time,
                processed_records,
                inserted_rows,
                rejected_rows,
                validation_failures,
                table_name,
                current_schema_version,
                target.history(1).select("version").first()["version"]
            )]

            df = (
                spark.createDataFrame(
                    row,
                    """
                    execution_start_time TIMESTAMP,
                    execution_end_time TIMESTAMP,
                    processed_records LONG,
                    inserted_records LONG,
                    rejected_records LONG,
                    validation_failures LONG,
                    table_name STRING,
                    schema_version LONG,
                    delta_version LONG
                    """
                )
            )

            df.write.format("delta").mode("append").saveAsTable(
                "pipeline_monitor"
            )

def read_pipeline_monitor(spark: SparkSession):
    """
    Read the pipeline monitor table and return a DataFrame.
    """
    initialize_pipeline_monitor_table(spark)
    return spark.table("pipeline_monitor")
