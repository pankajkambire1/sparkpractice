from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sparkConf = SparkConf()
sparkConf.set("spark.app.name","Simple Aggregate")
sparkConf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

invoiceDf = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path","/Users/Pankaj/Downloads/order_data.csv") \
    .load()

invoiceDf.select(
    count("*").alias("TotalRows"),
    sum("Quantity").alias("TotalQuantity"),
    avg("UnitPrice").alias("AverageUnit"),
    countDistinct("InvoiceNo").alias("distinctInvoiceNo")).show()

invoiceDf.selectExpr("count(*) as TotalRows",
                     "sum(Quantity) as TotalQuantity",
                     "avg(UnitPrice) as AverageUnit",
                     "count(distinct(InvoiceNo)) as distinctInvoiceNo").show()

invoiceDf.createTempView("sales")

spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()



# invoiceDf.show()

spark.stop()