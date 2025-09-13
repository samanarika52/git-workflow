# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipeline
# MAGIC
# MAGIC This Lakeflow Declarative Pipeline definition is executed using a pipeline defined in resources/gitaction_dabs_cicd.pipeline.yml.

# COMMAND ----------

# Import DLT and src/gitaction_dabs_cicd
import dlt
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr
from gitaction_dabs_cicd import main

# COMMAND ----------

@dlt.view
def taxi_raw():
    return main.find_all_taxis()


@dlt.table
def filtered_taxis():
    return dlt.read("taxi_raw").filter(expr("fare_amount < 30"))