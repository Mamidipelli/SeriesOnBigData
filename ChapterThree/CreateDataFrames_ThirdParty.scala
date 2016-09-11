package ch03

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

/**
  * Created by Giancarlo on 6/14/2016.
  */
object CreateDataFrames_ThirdParty {

  def setType(df: org.apache.spark.sql.DataFrame, name:String, newType:String):DataFrame = {
    val df_new = df.withColumnRenamed(name, "tmp")
    df_new.withColumn(name, df_new.col("tmp").cast(newType)).drop("tmp")
  }

  def setTypes(df: org.apache.spark.sql.DataFrame, name:Array[String], newType:Array[String]):DataFrame = {
    var dfTmp = df
    for ( i <- 0 to name.length-1){
      dfTmp = setType(dfTmp,name(i),newType(i))
    }
    dfTmp
  }

  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("DataFrames from RDD")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("c:/Spark/data/03-IntroductionToSpark/Retail/OnlineRetail_WithHeader.csv")
    df.printSchema()

    val columns = Array("InvoiceNo","StockCode","Description","Quantity","InvoiceDate","UnitPrice","CustomerID","Country")
    val types = Array("String","String","String","int","String","Float","String","String")
    val dataset = setTypes(df,columns,types)
    dataset.printSchema()

    val franceDF = dataset.select("InvoiceNo","Description","Quantity","Country").where(dataset("Country") === "France")

    franceDF.show(5)

    dataset.registerTempTable("Invoices")

    dataset.sqlContext.sql("SELECT COUNT(*) as total FROM Invoices WHERE Country='France'").show()

  }
}
