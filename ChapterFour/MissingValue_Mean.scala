package Ch04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.util.StatCounter

/**
  * Created by Giancarlo on 6/22/2016.
  */
object MissingValue_Mean {
  //Defines numerical types and SummaryClass
  val nTypes = Set("Float","Int","Double")

  class SummaryStat (m:Double, s:Double, mx:Double, mn:Double) extends Serializable {
    val AVG:Double = m
    val SD:Double = s
    val MAX:Double = mx
    val MIN:Double = mn
  }

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

  def extractStat(df: DataFrame, columns: Array[String], types: Array[String]): Map[String, SummaryStat] = {
    var result:Map[String, SummaryStat] = Map()
    for (idx <- 0 until columns.size) {
      if (nTypes.contains(types(idx))){
        val summary = df.select(mean(columns(idx)),stddev(columns(idx)),max(columns(idx)),min(columns(idx))).collect()
        val sAVG = summary(0).getAs[Double]("avg("+columns(idx)+")")
        val sSD = summary(0).getAs[Double]("stddev_samp("+columns(idx)+",0,0)")
        val sMAX = summary(0).getAs[Float]("max("+columns(idx)+")")
        val sMIN = summary(0).getAs[Float]("min("+columns(idx)+")")

        val summaryObj = new SummaryStat(sAVG,sSD,sMAX,sMIN)
        result+=(columns(idx)->summaryObj)
      }
    }
    result
  }

  def replaceWithMean(df: DataFrame, columns: Array[String], types: Array[String]): DataFrame = {
    val sStats = extractStat(df, columns, types)
    var result = df
    for (idx <- 0 until columns.size) {
      // Replace missing values with mean
      if (sStats.contains(columns(idx)))
        result = result.na.fill(sStats(columns(idx)).AVG, Array(columns(idx)))
    }
    result
  }

  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Data Preprocessing")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read the dataset
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("c:/Spark/data/04-DataPreprocessing/01_Abalone - Missing.csv")

    // Convert the schema to the appropriate types
    val columns = Array("Sex","Length","Diameter","Height","WholeWeight","ShuckedWeight","Vweight","ShellWeight","Rings")
    val types = Array("String","Float","Float","Float","Float","Float","Float","Float","int")
    val dataset = setTypes(df,columns,types)


    // Dropping records with missing values
    val countBefore = dataset.count()
    val cleanDataset = dataset.na.drop()
    val countAfter = cleanDataset.count()

    println("Removed #"+(countBefore-countAfter)+ " records.")

    // Replace Missing values with Mean
    dataset.describe().show()
    val dfMean = replaceWithMean(dataset, columns,types)
    dfMean.describe().show()
  }

}
