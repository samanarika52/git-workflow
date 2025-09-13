# Databricks notebook source
# Configure ADLS directly with service principal credentials
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Service principal credentials
client_id = "e4cdc288-4014-4ae0-a0f3-6ad578356929"
client_secret = "~tx8Q~gZ6jQTQIeAfFe0P8VFQUQt4gdlS_mOcaR1"
tenant_id = "7544b930-a681-489d-b056-a2465753106d"
storage_account = "dabsstorage"

# COMMAND ----------

# Configure Azure ADLS (this is working!)
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder.appName("BankData").getOrCreate()

# Service principal credentials
client_id = "e4cdc288-4014-4ae0-a0f3-6ad578356929"
tenant_id = "7544b930-a681-489d-b056-a2465753106d"
storage_account = "dabsstorage"

# Retrieve client secret from Databricks secret scope
client_secret = dbutils.secrets.get(scope="dabsscope", key="dabssecret")

# Configure Azure ADLS
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# Read the CSV files
print("📊 Reading bankdetails.csv...")
bank_full_df = spark.read.csv("abfss://dabcontainer@dabsstorage.dfs.core.windows.net/Raw/bankfullfordabs.csv", header=True, inferSchema=True)
print(f"Bank details schema: {bank_full_df.count()} rows, {len(bank_full_df.columns)} columns")

# COMMAND ----------

# Read the CSV files
print("📊 Reading bankdetails.csv...")
bank_additional_df = spark.read.csv("abfss://dabscontainer@dabsstorage.dfs.core.windows.net/raw/bank-additional-full.csv", header=True, inferSchema=True)
print(f"Bank details schema: {bank_additional_df.count()} rows, {len(bank_additional_df.columns)} columns")

# COMMAND ----------

# Show some data
print("\n🔍 Sample data from bankfullfordabs.csv:")
bank_full_df.show(5)

print("\n🔍 Sample data from bank-additional-full.csv:")
bank_additional_df.show(5)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder.appName("BankData").getOrCreate()

# Service principal credentials
client_id = "e4cdc288-4014-4ae0-a0f3-6ad578356929"
client_secret = "~tx8Q~gZ6jQTQIeAfFe0P8VFQUQt4gdlS_mOcaR1"
tenant_id = "7544b930-a681-489d-b056-a2465753106d"
storage_account = "dabsstorage"

# Configure Azure ADLS
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Define schema for bank-additional-full.csv to ensure correct column parsing
schema_additional = StructType([
    StructField("age", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("marital", StringType(), True),
    StructField("education", StringType(), True),
    StructField("default", StringType(), True),
    StructField("housing", StringType(), True),
    StructField("loan", StringType(), True),
    StructField("contact", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("campaign", IntegerType(), True),
    StructField("pdays", IntegerType(), True),
    StructField("previous", IntegerType(), True),
    StructField("poutcome", StringType(), True),
    StructField("emp.var.rate", DoubleType(), True),
    StructField("cons.price.idx", DoubleType(), True),
    StructField("cons.conf.idx", DoubleType(), True),
    StructField("euribor3m", DoubleType(), True),
    StructField("nr.employed", DoubleType(), True),
    StructField("y", StringType(), True)
])

# Read the CSV files
print("📊 Reading bankfullfordabs.csv...")
bank_full_df = spark.read.csv("abfss://dabscontainer@dabsstorage.dfs.core.windows.net/raw/bankfullfordabs.csv", header=True, inferSchema=True)
print(f"Bank full schema: {bank_full_df.count()} rows, {len(bank_full_df.columns)} columns")

print("📊 Reading bank-additional-full.csv...")
bank_additional_df = spark.read.csv(
    "abfss://dabscontainer@dabsstorage.dfs.core.windows.net/raw/bank-additional-full.csv",
    header=True,
    schema=schema_additional,  # Use explicit schema
    sep=';',
    quote='"'
)
print(f"Bank additional schema: {bank_additional_df.count()} rows, {len(bank_additional_df.columns)} columns")

# Show some data
print("\n🔍 Sample data from bankfullfordabs.csv:")
bank_full_df.show(5)

print("\n🔍 Sample data from bank-additional-full.csv:")
bank_additional_df.show(5)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mode, sum as F_sum, count as F_count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder.appName("BankData").getOrCreate()

# Service principal credentials
client_id = "e4cdc288-4014-4ae0-a0f3-6ad578356929"
client_secret = "~tx8Q~gZ6jQTQIeAfFe0P8VFQUQt4gdlS_mOcaR1"
tenant_id = "7544b930-a681-489d-b056-a2465753106d"
storage_account = "dabsstorage"

# Configure Azure ADLS
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Define schema for bank-additional-full.csv
schema_additional = StructType([
    StructField("age", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("marital", StringType(), True),
    StructField("education", StringType(), True),
    StructField("default", StringType(), True),
    StructField("housing", StringType(), True),
    StructField("loan", StringType(), True),
    StructField("contact", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("campaign", IntegerType(), True),
    StructField("pdays", IntegerType(), True),
    StructField("previous", IntegerType(), True),
    StructField("poutcome", StringType(), True),
    StructField("emp.var.rate", DoubleType(), True),
    StructField("cons.price.idx", DoubleType(), True),
    StructField("cons.conf.idx", DoubleType(), True),
    StructField("euribor3m", DoubleType(), True),
    StructField("nr.employed", DoubleType(), True),
    StructField("y", StringType(), True)
])

# Read the CSV files
print("📊 Reading bankfullfordabs.csv...")
bank_full_df = spark.read.csv("abfss://dabscontainer@dabsstorage.dfs.core.windows.net/raw/bankfullfordabs.csv", header=True, inferSchema=True)
print(f"Bank full schema: {bank_full_df.count()} rows, {len(bank_full_df.columns)} columns")
print("\n🔍 bank_full_df schema:")
bank_full_df.printSchema()

print("📊 Reading bank-additional-full.csv...")
bank_additional_df = spark.read.csv(
    "abfss://dabscontainer@dabsstorage.dfs.core.windows.net/raw/bank-additional-full.csv",
    header=True,
    schema=schema_additional,
    sep=';',
    quote='"'
)
print(f"Bank additional schema: {bank_additional_df.count()} rows, {len(bank_additional_df.columns)} columns")
print("\n🔍 bank_additional_df schema:")
bank_additional_df.printSchema()

# Rename 'day' to 'day_of_week' in bank_full_df to align with bank_additional_df
print("🔄 Renaming 'day' to 'day_of_week' in bank_full_df...")
bank_full_df = bank_full_df.withColumnRenamed("day", "day_of_week")

# Transformation 1: Combine DataFrames with Common Columns
print("📊 Combining bank_full_df and bank_additional_df on common columns...")
common_columns = [
    "age", "job", "marital", "education", "default", "housing", "loan",
    "contact", "month", "day_of_week", "duration", "campaign", "pdays",
    "previous", "poutcome", "y"
]

bank_full_selected = bank_full_df.select(common_columns)
bank_additional_selected = bank_additional_df.select(common_columns)
combined_df = bank_full_selected.union(bank_additional_selected)
print(f"Combined DataFrame: {combined_df.count()} rows, {len(combined_df.columns)} columns")

# Transformation 2: Handle Missing or Unknown Values
print("🧹 Replacing 'unknown' with mode in categorical columns...")
categorical_columns = ["job", "marital", "education", "default", "housing", "loan", "contact", "poutcome"]

for column in categorical_columns:
    mode_value = combined_df.groupBy(column).count().orderBy(col("count").desc()).first()[column]
    if mode_value == "unknown":
        mode_value = combined_df.groupBy(column).count().orderBy(col("count").desc()).filter(col(column) != "unknown").first()[column]
    combined_df = combined_df.withColumn(column, when(col(column) == "unknown", mode_value).otherwise(col(column)))

# Verify by showing a sample
print("\n🔍 Sample data after replacing 'unknown' values:")
combined_df.show(5)

# Transformation 3: Create a New Feature (High Campaign Contacts)
print("✨ Adding new feature: high_campaign (campaign > 3)...")
combined_df = combined_df.withColumn("high_campaign", when(col("campaign") > 3, 1).otherwise(0))

# Verify the new column
print("\n🔍 Sample data with high_campaign column:")
combined_df.select("campaign", "high_campaign").show(5)

# Transformation 4: Group and Aggregate by Job Type
print("📈 Aggregating subscription rate by job type...")
job_subscription = combined_df.groupBy("job").agg(
    (F_sum(when(col("y") == "yes", 1).otherwise(0)) / F_count(lit(1))).alias("subscription_rate")
).orderBy(col("subscription_rate").desc())

# Show the aggregation
print("\n🔍 Subscription rate by job type:")
job_subscription.show()

# Save the transformed DataFrame to ADLS in transformed folder
print("💾 Saving transformed DataFrame to ADLS transformed folder...")
combined_df.write.mode("overwrite").parquet(
    f"abfss://dabscontainer@dabsstorage.dfs.core.windows.net/transformed/combined_bank_data"
)
print("Transformed data saved successfully!")

# Save the aggregated DataFrame to ADLS in transformed folder
print("💾 Saving aggregated subscription rate to ADLS transformed folder...")
job_subscription.write.mode("overwrite").parquet(
    f"abfss://dabscontainer@dabsstorage.dfs.core.windows.net/transformed/job_subscription_rate"
)
print("Aggregated data saved successfully!")