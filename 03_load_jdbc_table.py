# Databricks notebook source
# =============================================================================
# load_jdbc_table.py — Reusable JDBC Loader for AdventureWorksLT
# =============================================================================
# Called by the For Each workflow task. Handles both partitioned and
# non-partitioned tables. Exits with structured JSON result.

# COMMAND ----------

import json

# COMMAND ----------

# --- Widgets (populated by the workflow task) ---
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("partition_column", "")
dbutils.widgets.text("num_partitions", "")
dbutils.widgets.text("fetchsize", "10000")
dbutils.widgets.text("mode", "overwrite")

# COMMAND ----------

# --- Read parameters ---
source_schema   = dbutils.widgets.get("source_schema")
source_table    = dbutils.widgets.get("source_table")
target_catalog  = dbutils.widgets.get("target_catalog")
target_schema   = dbutils.widgets.get("target_schema")
target_table    = dbutils.widgets.get("target_table")
partition_col   = dbutils.widgets.get("partition_column").strip()
num_partitions  = dbutils.widgets.get("num_partitions").strip()
fetchsize       = int(dbutils.widgets.get("fetchsize"))
write_mode      = dbutils.widgets.get("mode")

is_partitioned = partition_col != "" and num_partitions != ""

full_source = f"{source_schema}.{source_table}"
full_target = f"{target_catalog}.{target_schema}.{target_table}"

# COMMAND ----------

# --- JDBC connection (hardcoded) ---
jdbc_url = (
    "jdbc:sqlserver://sql-adventureworks-4e660b15.database.windows.net:1433;"
    "database=AdventureWorksLT;encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)
jdbc_user = "adventureworks_reader"
jdbc_pass = dbutils.secrets.get(scope="jdbc", key="adventureworks-password")

DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# --- Load table ---

try:
    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", full_source)
        .option("user", jdbc_user)
        .option("password", jdbc_pass)
        .option("driver", DRIVER)
        .option("fetchsize", fetchsize)
    )

    if is_partitioned:
        num_parts = int(num_partitions)

        bounds_query = (
            f"(SELECT MIN({partition_col}) AS lo, MAX({partition_col}) AS hi "
            f"FROM {full_source}) AS bounds"
        )
        bounds = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", bounds_query)
            .option("user", jdbc_user)
            .option("password", jdbc_pass)
            .option("driver", DRIVER)
            .load()
            .collect()[0]
        )
        lower_bound = bounds["lo"]
        upper_bound = bounds["hi"]

        print(f"Loading {full_source} -> {full_target} (PARTITIONED)")
        print(f"  Column: {partition_col}, Bounds: [{lower_bound} .. {upper_bound}], Partitions: {num_parts}")

        reader = (
            reader
            .option("partitionColumn", partition_col)
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
            .option("numPartitions", num_parts)
        )
    else:
        print(f"Loading {full_source} -> {full_target} (SINGLE CONNECTION)")

    df = reader.load()

    # Write to Delta / Unity Catalog
    df.write.format("delta").mode(write_mode).option("overwriteSchema", "true").saveAsTable(full_target)

    row_count = spark.table(full_target).count()
    print(f"Done: {full_target} — {row_count} rows")

    result = json.dumps({
        "table": full_target,
        "row_count": row_count,
        "status": "success",
    })

except Exception as e:
    print(f"FAILED: {full_target} — {str(e)}")
    result = json.dumps({
        "table": full_target,
        "row_count": 0,
        "status": "failed",
        "error": str(e),
    })

# COMMAND ----------

dbutils.notebook.exit(result)
