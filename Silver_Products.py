# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
      .load("abfss://bronze@databricksetebs.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql 
# MAGIC select product_id, price,  databricks_cata.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql 
# MAGIC select product_id, brand, databricks_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path", "abfss://silver@databricksetebs.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.products_silver 
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksetebs.dfs.core.windows.net/products'

# COMMAND ----------

