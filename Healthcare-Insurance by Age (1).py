# Databricks notebook source
access_key = 'AKIA2UMQPZOWKBGCLTIR'
secret_key = 'Id0AYvUEybwTYFQu6TcIuotR9x8kwX25B/xFShI3'
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name_raw = "health-insurance-data-raw"
aws_bucket_name_cleaned = "healthcare-analytics-cleaned"
mount_name_raw = "health-insurance-data-raw-bucket"
mount_name_clean = "health-insurance-data-clean-bucket"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name_raw}", f"/mnt/{mount_name_raw}")
dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name_cleaned}", f"/mnt/{mount_name_clean}")

file_location = "dbfs:/mnt/health-insurance-data-raw-bucket/Rate.csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.csv(file_location, inferSchema = True, header= True)
df.printSchema()


# COMMAND ----------

#remove # to unmount files.
#dbutils.fs.unmount(f"/mnt/{mount_name_raw}")
#dbutils.fs.unmount(f"/mnt/{mount_name_clean}")
# OR #

# COMMAND ----------

# Selecting specific columns from the DataFrame
rate_df = df.select("BusinessYear", "PlanId", "StateCode", "Age")

# Displaying the new DataFrame
display(rate_df)


# COMMAND ----------

# Grouping by 'Age', counting occurrences, and ordering by 'Age'
filterByAge_df = rate_df.groupBy('Age').count().orderBy('Age')

# Displaying the new DataFrame
display(filterByAge_df)


# COMMAND ----------

# Writing the rate_df DataFrame to a CSV file in S3
rate_df.write.option("header", "true").option("inferSchema", "true").csv('/mnt/health-insurance-data-clean-bucket/Age_wise/')


# COMMAND ----------

import matplotlib.pyplot as plt


filterByAge_df = rate_df.groupBy('Age').count().orderBy('Age').toPandas()

# Plot the data using Matplotlib
filterByAge_df.plot(kind='bar', x='Age', y='count', figsize=(18, 5))
plt.ylabel("Benefit Count")
plt.xlabel("Age")
plt.title("Benefit Count by Age")
plt.xticks(rotation=0)
plt.show()


# COMMAND ----------


