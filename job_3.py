from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark
import csv
from pyspark.sql.functions import *

conf = SparkConf().setAppName("Job J3").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

try:
    file1 = input("Enter the source/file path of the 1st file: ")
    file2 = input("Enter the source/file path of the 2nd file: ")
    destination = input("Enter the destination path: ")
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('../'+file1)
    df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('../'+file2)
    cond = [df.product == df1.product, df.company == df1.company]
    df2=df.join(df1, cond).select(df.product,df.company,df.total_no,df1.outcome,df1.no_outcome)
    df2.coalesce(1).write.option("header","true").csv(destination)
    df2.show()

except FileNotFoundError:
    print("File not found!")

except:
    print("Some error occured!")
