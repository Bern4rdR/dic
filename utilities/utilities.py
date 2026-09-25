
from pathlib import Path

def find_project_root() -> Path:
    path = Path(__file__).resolve()

    for parent in [path, *path.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise RuntimeError("Could not find project root")

PROJECT_ROOT = find_project_root()

def drop_all_tables(spark):
    """
    Drop all tables in the Spark catalog.
    """
    for table in spark.catalog.listTables():
        spark.sql(f"DROP TABLE IF EXISTS {table.name}")

