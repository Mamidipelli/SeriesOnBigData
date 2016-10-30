package Ch04

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Giancarlo on 9/11/2016.
  */
object SummaryStatistics_StatCounter {

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

  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Data Preprocessing")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Read the dataset
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("c:/Spark/data/04-DataPreprocessing/01_Abalone - Missing.csv")

    // Convert the schema to the appropriate types
    val columns = Array("Sex","Length","Diameter","Height","WholeWeight","ShuckedWeight","Vweight","ShellWeight","Rings")
    val types = Array("String","Float","Float","Float","Float","Float","Float","Float","int")
    val dataset = setTypes(df,columns,types)

    // Summary Statistics for Length
    val lengthRDD = dataset.select("Length").rdd.map(row => row.getFloat(0))

    // The easy way
    println ("Mean: "+lengthRDD.mean())
    println ("Max: "+lengthRDD.max())
    println ("Min: "+lengthRDD.min())
    println ("SD: "+lengthRDD.stdev())

    // With the StatsObject
    val stats = lengthRDD.stats()

    println("Mean: " + stats.mean)
    println("SD: " + stats.stdev)
    println("Min: " + stats.min)
    println("Max: " + stats.max)

  }
}
