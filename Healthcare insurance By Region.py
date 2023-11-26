# Databricks notebook source
#access_key = 'access_key'
#secret_key = 'secret_key'
#sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",access_key)
#sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key",secret_key)

#aws_region = "us-east-1"
#sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint","s3."+aws_region+".amazonaws.com")
#BenefitsCostSharing_df = spark.read.csv('s3://health-insurance-data-raw/BenefitsCostSharing.csv',inferSchema=True,header=True)
#Rate_df = spark.read.csv('s3://health-insurance-data-raw/Rate.csv',inferSchema=True,header=True)
#ServiceArea_df = spark.read.csv('s3://health-insurance-data-raw/ServiceArea.csv',inferSchema=True,header=True)


access_key = 'access_key'
secret_key = 'secret_key'
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name_raw = "health-insurance-data-raw"
aws_bucket_name_cleaned = "healthcare-analytics-cleaned"
mount_name_raw = "health-insurance-data-raw-bucket"
mount_name_clean = "health-insurance-data-clean-bucket"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name_raw}", f"/mnt/{mount_name_raw}")
dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name_cleaned}", f"/mnt/{mount_name_clean}")


file_location = "dbfs:/mnt/health-insurance-data-raw-bucket/BenefitsCostSharing.csv"



# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.csv(file_location, inferSchema = True, header= True)
df.printSchema()

# COMMAND ----------

#to unmount S3 file
#dbutils.fs.unmount(f"/mnt/{mount_name_raw}")
#dbutils.fs.unmount(f"/mnt/{mount_name_clean}")
# OR #
# dbutils.fs.unmount(f"/mnt/{mount_name}")

# COMMAND ----------

BenefitsCostSharing_df = df
BenefitsCostSharing_df.printSchema()

# COMMAND ----------

BCS_df = BenefitsCostSharing_df.select("BenefitName","BusinessYear","PlanId","StateCode")
display(BCS_df)


# COMMAND ----------

BCS_CountbyBusinessYear_df = BCS_df.groupBy('BusinessYear').count()
display(BCS_CountbyBusinessYear_df)

# COMMAND ----------

#remove data where BusinessYear is null
BCS_df = BCS_df.filter("BusinessYear=2014 OR BusinessYear=2015 OR BusinessYear=2016")
BCS_CountbyBusinessYear_df = BCS_df.groupBy('BusinessYear').count()
display(BCS_CountbyBusinessYear_df)


# COMMAND ----------

display(BCS_df)

# COMMAND ----------

from pyspark.sql.functions import col
BCS_CountbyStateCode_df = BCS_df.groupBy('StateCode').count()
#BCS_CountbyStateCode_df = BCS_CountbyStateCode_df.withColumnRenamed('count', 'Counts')
#BCS_CountbyStateCode_df = BCS_CountbyStateCode_df.filter(BCS_CountbyStateCode_df.Counts > 1000).select()
filtered_statecodes = BCS_CountbyStateCode_df.filter(col('count') >= 1000)

# Step 3: Extract StateCodes to be retained
statecodes_to_keep = [row['StateCode'] for row in filtered_statecodes.collect()]

# Step 4: Filter the original DataFrame based on the retained StateCodes
filtered_BCS_df = BCS_df.filter(col('StateCode').isin(statecodes_to_keep))
# Step 5: Show all columns in the filtered DataFrame
display(filtered_BCS_df)
# Display the filtered DataFrame


# COMMAND ----------

#verifying the transformation logic output.
BCS_CountbyStateCode_df = filtered_BCS_df.groupBy('StateCode').count()
display(BCS_CountbyStateCode_df)

# COMMAND ----------

#BCS_CountbyStateCode_df.write.save(f'/mnt/health-insurance-data-clean-bucket/region_wise/',format='csv')
#filtered_BCS_df.write.option("header", "true").option("inferSchema", "true").csv('/mnt/health-insurance-data-clean-bucket/region_wise/')



# COMMAND ----------

import matplotlib.pyplot as plt

# Assuming you have a DataFrame named 'filterByAge_df' with columns 'statecode' and 'count'
# Replace 'your_data.csv' with your actual data source
BCS_CountbyStateCode_df = filtered_BCS_df.groupBy('StateCode').count().orderBy('count').toPandas()

# Plot the data using Matplotlib
BCS_CountbyStateCode_df.plot(kind='bar', x='StateCode', y='count', figsize=(18, 5))
plt.ylabel("Benefit Count")
plt.xlabel("StateCode")
plt.title("Benefit Count by Region")
plt.xticks(rotation=0)
plt.show()



# COMMAND ----------


