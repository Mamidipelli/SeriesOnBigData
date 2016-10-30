package Ch04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import scala.collection.immutable.HashMap

import org.stjohns.utils.DataFrameUtil

/**
  * Created by Giancarlo on 10/12/2016.
  */
object MissingValue_Random {
  //Defines numerical types and SummaryClass
  val nTypes = Set("Float","Int","Double")
  val columns = Array("Sex","Length","Diameter","Height","WholeWeight","ShuckedWeight","Vweight","ShellWeight","Rings")
  val types = Array("String","Float","Float","Float","Float","Float","Float","Float","int")
  var dataframe = null

  def getRandomSample (rdd:RDD[Row], sampleSize:Int):Array[Float] ={
    val rGenerator = scala.util.Random
    val sample = rdd.takeSample(false,sampleSize,rGenerator.nextLong()).map(row=> try {row.getFloat(0)} catch {case e: Exception => row.getInt(0)})
    sample
  }

  def processRow(row:Row, samples:HashMap[String,Array[Float]]): Row ={
    var newRowArray = new Array[Any](columns.size)

    for (idx <- columns.indices) {
      if (row.isNullAt(idx)) {
        //println("Found Missing Value ["+columns(idx)+"]")
        // Generate random seed
        val rGenerator = scala.util.Random

        // Get a random value
        val pValues = samples(columns(idx))
        val sample = pValues(rGenerator.nextInt(pValues.length))
        // Replace Missing Value with random
        newRowArray(idx) = sample
      } else {
        newRowArray(idx) = row.get(idx)
        //println("["+columns(idx)+"] is ok")
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
    val dataset = DataFrameUtil.setTypes(df,columns,types)

    dataset.describe().show()

    // Replace Missing values with Random

    //Retrieving random sample for each column
    var samples = HashMap[String,Array[Float]]()
    val sampleSize = 10
    for (idx <- 0 until columns.size) {
      val rdd = dataset.select(columns(idx)).rdd
      if (!types(idx).equals("String"))
        samples += (columns(idx)) -> getRandomSample(rdd,sampleSize)
    }

    // Replace null values with random entries
    val rows = dataset.map(row => processRow(row,samples))

    // Create the new dataset
    val newDF = sqlContext.createDataFrame(rows,dataset.schema)

    newDF.describe().show()

    sc.stop()
  }


}
