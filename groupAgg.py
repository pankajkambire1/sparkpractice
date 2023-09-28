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

invoiceDf.groupBy("country","invoiceNo") \
    .agg(sum("Quantity").alias("TotalQuantity"),
         sum(expr("Quantity * UnitPrice")).alias("InvoiceValue")).show()

invoiceDf.groupBy("country","invoiceNo") \
    .agg(expr("sum(Quantity) as TotalQuantity"),
         expr("sum(Quantity * UnitPrice) as InvoiceValue")).show()

invoiceDf.createTempView("sales")

spark.sql("""select country,
invoiceNo,sum(Quantity) as TotalQuantity,
sum(Quantity * UnitPrice) as InvoiceValue
from sales group by country,invoiceNo""").show()

spark.stop()