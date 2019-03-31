from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import Row
import csv

conf = SparkConf().setAppName("Job J2").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

try:
    source = input("Enter the source/file path: ")
    destination = input("Enter the destination path: ")
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(source)
    df2 = df.select('company','product','company_response_to_consumer')
    df3 = df2.groupBy('company','product','company_response_to_consumer').count()
    df3.coalesce(1).write.option("header","true").csv(destination)
    df3.show()

except FileNotFoundError:
    print("File not found!")

except:
    print("Some error occured!")
