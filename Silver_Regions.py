# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetebs.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta").load("abfss://silver@databricksetebs.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions_silver 
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetebs.dfs.core.windows.net/regions'