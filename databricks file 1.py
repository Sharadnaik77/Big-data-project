# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

storage_account = "olistdatastoragesharad"
application_id = "d6e56c5c-42a7-47e0-940f-7ca216b14671"
directory_id = "754579ea-3194-4bb3-878a-8bad314ef2e6"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "ZXY8Q~1H~F9PpwQhowfm4A9d4OyrKCB114FzSbNV")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #reading data from ADLS

# COMMAND ----------

customers_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_customers_dataset.csv")


# COMMAND ----------

display(df)

# COMMAND ----------

geolocation_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv")

order_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_order_list_dataset.csv")

payment_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv")

order_review_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv")

orders_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_orders_dataset.csv")

product_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_products_dataset.csv")

orders_items_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_order_items_dataset.csv")


seller_df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/bronze/olist_sellers_dataset.csv")


# COMMAND ----------

import pymongo

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading data from pymongo

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "9edsv.h.filess.io"
database = "olistdataNoSQL_compareice"
port = "61004"
username = "olistdataNoSQL_compareice"
password = "7f1e4d8796b0b613a62e2137bbdbd1622bf32622"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_category_translation']

mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ###cleaning data

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date,when

# COMMAND ----------

def clean_datafram(df, name):
    print("cleaning"+name)
    return df.dropDuplicates().na.drop('all')

orders_df = clean_datafram(orders_df,"orders")
display(orders_df)

# COMMAND ----------

#conver date colums

orders_df = orders_df.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date",to_date("order_delivered_customer_date"))\
    .withColumn("order_estimated_delivery_date",to_date("order_estimated_delivery_date"))


# COMMAND ----------

#calculate delivery and time delays

orders_df = orders_df.withColumn("delivery_delay",datediff(col("order_delivered_customer_date"),col("order_purchase_timestamp")))
orders_df = orders_df.withColumn("estimated_delivery_delay",datediff(col("order_estimated_delivery_date"),col("order_purchase_timestamp")))
orders_df = orders_df.withColumn("delay time", (col("delivery_delay") - col("estimated_delivery_delay")))

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Joining
# MAGIC
# MAGIC

# COMMAND ----------

orders_items_df.columns

# COMMAND ----------

orders_customers_df = orders_df.join(customers_df,orders_df.customer_id == customers_df.customer_id,"left")

orders_payments_df = orders_customers_df.join(payment_df,orders_customers_df.order_id == payment_df.order_id,"left")

orders_items_df = orders_payments_df.join(orders_items_df,"order_id","left")

orders_items_products_df = orders_items_df.join(product_df,orders_items_df.product_id == product_df.product_id,"left")

final_df = orders_items_products_df.join(seller_df,orders_items_products_df.seller_id == seller_df.seller_id,"left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

 mongo_data.drop('_id', axis=1, inplace=True)

# Convert the pandas DataFrame to a Spark DataFrame
mongo_spark_df = spark.createDataFrame(mongo_data)

# Display the Spark DataFrame
display(mongo_spark_df)

# COMMAND ----------

final_df = final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns
    
    seen_colums = set()
    column_to_drop = []

    for column in columns:
        if column in seen_colums:
            column_to_drop.append(column)
        else:
            seen_colums.add(column)

    df_cleaned = df.drop(*column_to_drop)
    return df_cleaned


final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df = final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastoragesharad.dfs.core.windows.net/silver")
