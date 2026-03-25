# Databricks notebook source
# =============================================================================
# generate_table_list.py — Table Configs + Workflow Definition for AdventureWorksLT
# =============================================================================
# Defines 10 AdventureWorksLT tables (5 partitioned, 5 non-partitioned),
# optionally deploys a workflow, and exits with JSON for the For Each task.

# COMMAND ----------

import json

# COMMAND ----------

# --- Widget: toggle workflow deployment ---
dbutils.widgets.dropdown("deploy_workflow", "no", ["yes", "no"], "Deploy Workflow?")

# COMMAND ----------

# --- Table configurations ---
# Partitioned tables get parallel JDBC reads; non-partitioned use a single connection.

table_configs = [
    # =========================================================================
    # PARTITIONED TABLES — parallel JDBC reads
    # =========================================================================
    {
        "source_schema": "SalesLT",
        "source_table": "Customer",
        "target_catalog": "km_demo",
        "target_schema": "sales",
        "target_table": "customer",
        "partition_column": "CustomerID",
        "num_partitions": 8,
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "SalesOrderHeader",
        "target_catalog": "km_demo",
        "target_schema": "sales",
        "target_table": "sales_order_header",
        "partition_column": "SalesOrderID",
        "num_partitions": 8,
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "SalesOrderDetail",
        "target_catalog": "km_demo",
        "target_schema": "sales",
        "target_table": "sales_order_detail",
        "partition_column": "SalesOrderDetailID",
        "num_partitions": 16,
        "fetchsize": 5000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "Address",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "address",
        "partition_column": "AddressID",
        "num_partitions": 8,
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "Product",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "product",
        "partition_column": "ProductID",
        "num_partitions": 4,
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    # =========================================================================
    # NON-PARTITIONED TABLES — single JDBC connection
    # =========================================================================
    {
        "source_schema": "SalesLT",
        "source_table": "ProductCategory",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "product_category",
        "partition_column": "",
        "num_partitions": "",
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "ProductModel",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "product_model",
        "partition_column": "",
        "num_partitions": "",
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "ProductDescription",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "product_description",
        "partition_column": "",
        "num_partitions": "",
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "ProductModelProductDescription",
        "target_catalog": "km_demo",
        "target_schema": "reference",
        "target_table": "product_model_product_description",
        "partition_column": "",
        "num_partitions": "",
        "fetchsize": 10000,
        "mode": "overwrite",
    },
    {
        "source_schema": "SalesLT",
        "source_table": "CustomerAddress",
        "target_catalog": "km_demo",
        "target_schema": "sales",
        "target_table": "customer_address",
        "partition_column": "",
        "num_partitions": "",
        "fetchsize": 10000,
        "mode": "overwrite",
    },
]

# COMMAND ----------

# --- Serialize configs (all values as strings for workflow parameter passing) ---

serializable_configs = []
for cfg in table_configs:
    serializable_configs.append({
        k: str(v) if v is not None else "" for k, v in cfg.items()
    })

print(f"Prepared {len(serializable_configs)} table configs:")
for cfg in serializable_configs:
    partitioned = "PARTITIONED" if cfg["partition_column"] else "single"
    print(f"  {cfg['source_schema']}.{cfg['source_table']} -> {cfg['target_catalog']}.{cfg['target_schema']}.{cfg['target_table']} ({partitioned})")

# COMMAND ----------

# --- Workflow definition ---

for_each_workflow = {
    "name": "adventureworks_parallel_load",
    "tasks": [
        {
            "task_key": "generate_list",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/kishore.mannava@databricks.com/whitecase/generate_table_list",
            },
            "environment_key": "default",
        },
        {
            "task_key": "for_each_load",
            "depends_on": [{"task_key": "generate_list"}],
            "for_each_task": {
                "inputs": "{{tasks.generate_list.values.table_list}}",
                "concurrency": 5,
                "task": {
                    "task_key": "load_table",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Users/kishore.mannava@databricks.com/whitecase/load_jdbc_table",
                        "base_parameters": {
                            "source_schema":    "{{input.source_schema}}",
                            "source_table":     "{{input.source_table}}",
                            "target_catalog":   "{{input.target_catalog}}",
                            "target_schema":    "{{input.target_schema}}",
                            "target_table":     "{{input.target_table}}",
                            "partition_column": "{{input.partition_column}}",
                            "num_partitions":   "{{input.num_partitions}}",
                            "fetchsize":        "{{input.fetchsize}}",
                            "mode":             "{{input.mode}}",
                        },
                    },
                    "environment_key": "default",
                },
            },
        },
    ],
    "environments": [
        {
            "environment_key": "default",
            "spec": {
                "environment_version": "4",
            },
        },
    ],
}

print("\nWorkflow JSON definition:")
print(json.dumps(for_each_workflow, indent=2))

# COMMAND ----------

# --- Optional: Deploy workflow via REST API ---

deploy = dbutils.widgets.get("deploy_workflow")

if deploy == "yes":
    import requests

    host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.post(
        f"{host}/api/2.1/jobs/create",
        headers=headers,
        json=for_each_workflow,
    )
    response.raise_for_status()
    job_id = response.json().get("job_id")
    print(f"Deployed workflow job_id = {job_id}")
else:
    print("Workflow deployment skipped (set deploy_workflow=yes to deploy)")

# COMMAND ----------

# Set task value for the For Each task, then exit
dbutils.jobs.taskValues.set(key="table_list", value=serializable_configs)
dbutils.notebook.exit(json.dumps(serializable_configs))
