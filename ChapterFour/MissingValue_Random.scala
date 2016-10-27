package Ch04

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import org.apache.spark.sql.functions.udf

import scala.collection.immutable.HashMap

/**
  * Created by Giancarlo on 10/12/2016.
  */
object MissingValue_Random {
  //Defines numerical types and SummaryClass
  val nTypes = Set("Float","Int","Double")
  val columns = Array("Sex","Length","Diameter","Height","WholeWeight","ShuckedWeight","Vweight","ShellWeight","Rings")
  val types = Array("String","Float","Float","Float","Float","Float","Float","Float","int")

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

  def getRandomSample (rdd:RDD[Row], sampleSize:Int):Array[Float] ={
    val rGenerator = scala.util.Random
    val sample = rdd.takeSample(false,sampleSize,rGenerator.nextLong()).map(row=> try {row.getFloat(0)} catch {case e: Exception => row.getInt(0)})
    sample
  }

  def processRow(row:Row, samples:HashMap[String,Array[Float]]): Row ={
    var newRowArray = new Array[Any](columns.size)

    for (idx <- 0 until columns.size) {
      if (row.isNullAt(idx)) {
        // Generate random seed
        val rGenerator = scala.util.Random

        // Get a random value
        val pValues = samples(columns(idx))
        val sample = pValues(rGenerator.nextInt(pValues.length))
        // Replace Missing Value with random
        newRowArray(idx) = sample
      } else {
        newRowArray(idx) = row.get(idx)
      }
    }
    val newRow = Row.fromSeq(newRowArray.toSeq)
    newRow
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
    val dataset = setTypes(df,columns,types)

    // Replace Missing values with Random
    dataset.describe().show()

    //Retrieving random sample for each column
    var samples = HashMap[String,Array[Float]]()
    val sampleSize = 10
    for (idx <- 0 until columns.size) {
      val rdd = dataset.select(columns(idx)).rdd
      if (!types(idx).equals("String"))
        samples += (columns(idx)) -> getRandomSample(rdd,sampleSize)
    }

    // Replace missing value with random observation
    val rows = dataset.map(row => processRow(row,samples))
    val newDF = sqlContext.createDataFrame(rows,dataset.schema)

    newDF.describe().show()

    sc.stop()
  }
}
