import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object simpleAggregate extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
val  sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Simple Agggregation")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
 
 val invoiceDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","/Users/Pankaj/Downloads/order_data.csv")
  .load()
  
//  invoiceDf.show()
  
  invoiceDf.select(
       count("*").as("RowCount")
       ,sum("Quantity").as("TotalQuantity")
       ,avg("UnitPrice").as("AveragePrice")
       ,countDistinct("InvoiceNo").as("CountDistinct")
      ).show
      
      
   invoiceDf.selectExpr(
       "count(StockCode) as RowCount",
       "sum(Quantity) as TotalQuantity",
       "avg(UnitPrice) as AveragePrice",
       "count(Distinct(InvoiceNo)) as CountDistinct"
   ).show
  
  
  invoiceDf.createOrReplaceTempView("sales")
  
  spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show
  
  
  
  
  
  
  
  
  
  spark.stop()
  
}