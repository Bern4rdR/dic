import pyspark

from pathlib import Path

from pathlib import Path

def find_project_root() -> Path:
    path = Path(__file__).resolve()

    for parent in [path, *path.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise RuntimeError("Could not find project root")

PROJECT_ROOT = find_project_root()

WAREHOUSE_DIR = PROJECT_ROOT / "spark_project/spark-warehouse"
METASTORE_DIR = PROJECT_ROOT / "spark_project/metastore_db"

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

	# Disable hive column checks
     .config(
        "spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes",
        "false"
    )
    .config(
        "hive.metastore.disallow.incompatible.col.type.changes",
        "false"
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
