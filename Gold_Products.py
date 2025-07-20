# Databricks notebook source
# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Table**

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Expectation
my_rules = {
    "rule1" : "product_id is not null",
    "rule2" : "product_name is not null"
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    
    df = spark.readstream.table("databricks_cata.silver.products_silver")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming View**

# COMMAND ----------

@dlt.view

def DimProducts_view():
    df = spark.readstream.table("Live.DimProducts_stage")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimProdcuts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProducts_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2,
)