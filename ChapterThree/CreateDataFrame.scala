package ch03

import java.sql.Date
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Giancarlo on 9/10/2016.
  */
object CreateDataFrame {
  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("DataFrames from RDD")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val format = new java.text.SimpleDateFormat("dd/MM/yyyy")

    val rdd = sc.textFile("c:/Spark/data/03-IntroductionToSpark/Retail/OnlineRetail.csv")
      .map(line=>line.split(","))
      .map(fields=>(fields(0),fields(1),fields(2),fields(3).toInt,new Date(format.parse(fields(4)).getTime()),fields(5).toFloat,fields(6),fields(7)))

    // Providing the names for the columns
    val df = rdd.toDF("InvoiceNum","Skus","Desc","Qty","Date","uPrice","cID","Country")

    df.printSchema()

    val franceDF = df.select("InvoiceNum","Desc","Qty","Country").where(df("Country") === "France")
    franceDF.show(5)

    df.registerTempTable("Invoices")
    df.sqlContext.sql("SELECT COUNT(*) as total FROM Invoices WHERE Country='France'").show()
  }
}
