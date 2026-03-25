# Databricks notebook source
# =============================================================================
# setup.py — Connection & Catalog Setup for AdventureWorksLT
# =============================================================================
# Creates Unity Catalog schemas and validates JDBC connectivity.
# Idempotent — safe to re-run.

# COMMAND ----------

# JDBC connection details (hardcoded)
JDBC_URL = (
    "jdbc:sqlserver://sql-adventureworks-4e660b15.database.windows.net:1433;"
    "database=AdventureWorksLT;encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)
JDBC_USER = "adventureworks_reader"
JDBC_PASSWORD = "***REDACTED***"

# COMMAND ----------

# --- Step 1: Create Unity Catalog structures (idempotent) ---

spark.sql("USE CATALOG km_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS km_demo.sales")
spark.sql("CREATE SCHEMA IF NOT EXISTS km_demo.reference")

print("Unity Catalog ready:")
print("  - km_demo (catalog)")
print("  - km_demo.sales (schema)")
print("  - km_demo.reference (schema)")

# COMMAND ----------

# --- Step 2: Validate JDBC connectivity ---

test_df = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "(SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SalesLT') AS t")
    .option("user", JDBC_USER)
    .option("password", JDBC_PASSWORD)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

print("JDBC connectivity validated. SalesLT tables found:")
test_df.show(truncate=False)

dbutils.notebook.exit("setup_complete")
