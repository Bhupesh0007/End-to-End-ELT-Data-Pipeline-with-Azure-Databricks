# Databricks notebook source
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading from Source**

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.customers_silver")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Removing Duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])
# df.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **Dividing New vs Old Records**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql(''' select DimCustomerKey, customer_id, create_date, update_date 
                           from databricks_cata.gold.DimCustomer ''')

else: 
    df_old = spark.sql(''' select 0 DimCustomerKey, 0 customer_id,0 create_date, 0 update_date 
                           from databricks_cata.silver.customers_silver where 1=0 ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Renaming Columns of df_old**

# COMMAND ----------

df_old.display()

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
            .withColumnRenamed("customer_id", "old_customer_id")\
            .withColumnRenamed("create_date", "old_create_date")\
            .withColumnRenamed("update_date", "old_update_date")
           

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Join with the Old Records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], "left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Seperating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_old**

# COMMAND ----------

# Dropping all the columns which are not required
df_old = df_old.drop('old_customer_id','old_update_date')

# Renaming "old_DimCustomerKey" column to "DimCustomerKey"
df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# Renaming "old_create_date" column to "create_date"
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col('create_date')))

# Recreating "update_date" column with current timestamp
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_new**

# COMMAND ----------

# Dropping all the columns which are not required
df_new = df_new.drop('old_DimCustomerKey','old_customer_id','old_update_date','old_create_date')

# Recreating "update_date", "current_date" column with current timestamp
df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Surrogate Key - from 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id() + lit(1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# df = df.drop("DimCustomerKey")

# from pyspark.sql.functions import row_number
# from pyspark.sql.window import Window

# windowSpec = Window.orderBy("customer_id")
# df = df.withColumn("DimCustomerKey", row_number().over(windowSpec))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Adding Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomer")
    # Converting df_maxsur to max_surrogate_key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key) + col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union of df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type - 1**

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomer"):
    print("hello")

else:
    print("world")

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomer"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databricksetebs.dfs.core.windows.net/DimCustomer")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
   df_final.write.mode("overwrite")\
        .format("delta")\
        .option("path", "abfss://gold@databricksetebs.dfs.core.windows.net/DimCustomer")\
        .saveAsTable("databricks_cata.gold.DimCustomer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomer