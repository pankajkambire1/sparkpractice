import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object groupAggregate extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","Group Aggregate")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
  val invoiceDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","/Users/Pankaj/Downloads/order_data.csv")
  .load()
  
  
  
 val summaryDf =  invoiceDf.groupBy("Country","InvoiceNo")
  .agg(sum("Quantity").as("totalQuantity")
      ,sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
      )
  
  
  summaryDf.show
  
  
  val summary2Df = invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
       expr("sum(Quantity * UnitPrice) as InvoiceValue")    
  )
  
  summary2Df.show
  
  invoiceDf.createOrReplaceTempView("sales")
  
  val summary3Df = spark.sql("select Country,InvoiceNo,sum(Quantity) as TotalQuantity,sum(Quantity * UnitPrice) as InvoiceValue from sales group by Country,InvoiceNo")
  
  summary3Df.show
  
  
  spark.stop()
  
  
  
  
  
  
  
  
  
  
  
}