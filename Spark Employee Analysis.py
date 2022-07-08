# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.functions import sum,avg,max,min,mean,count
spark = SparkSession.builder.appName("Mini Project").getOrCreate()

# Load

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeDataProject.csv')
df.show()

# 1 Count
df.count()

# 2 Select

df.select("department").dropDuplicates(["department"]).count()

# 3 Drop Duplicates

df.select("department").dropDuplicates(["department"]).show()

# 4 Group By Department

df.groupBy('department').count().show()

# 5 Group By State

df.groupBy('state').count().show()

# 6 Group By State and Department 

df.groupBy('state','department').count().show()

df.show()

# 7 Group By, Aggregations, and Order By

df.groupBy("department").agg(min("salary").alias("min"), max("salary").alias("max")).orderBy(col("max").asc(), col("min").asc()).show()

df.show()

# 8 Filtering

avgBonus = df.filter(df.state == "NY").groupBy("state").agg(avg("bonus").alias("avg_bonus")).select("avg_bonus").collect()[0]['avg_bonus']
df.filter((df.state == "NY") & (df.department == "Finance") & (df.bonus > avgBonus)).show()

# 9 UDF and WithColumn

def incr_salary(age, currentSalary):
  if age > 45:
    return currentSalary + 500
  return currentSalary

incrSalaryUDF = udf(lambda x,y : incr_salary(x,y), IntegerType())


df.withColumn("salary", incrSalaryUDF(col("age"), col("salary"))).show()

# 10 Filter and Write to CSV

df.filter(df.age > 45).write.csv("/FileStore/tables/output_45")
