package Ch04

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.stjohns.utils.StatisticsUtil.SummaryStat
import org.stjohns.utils.{DataFrameUtil, StatisticsUtil}
/**
  * Created by Giancarlo on 10/18/2016.
  */
object Ouliers {

  def processRow(row:Row, sStats:Map[Int, SummaryStat]): Row ={
    val newRowArray = new Array[Any](row.size)

    for (idx <- 0 to row.length-1) {
      val zScore = (row.getFloat(idx)-sStats(idx).AVG)/sStats(idx).SD
      newRowArray(idx) = zScore
    }

    Row.fromSeq(newRowArray.toSeq)
  }

  def isOutlier(row:Row,lowerBound:Double,upperBound:Double): Boolean = {
    var result = false
    for (idx <- 0 to row.length-1) {
      val value = try {row.getDouble(0)} catch { case e:Exception => row.getFloat(0)}
      if (value<lowerBound || value>upperBound)
        result = true
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
    val dataset = DataFrameUtil.setTypes(df,columns,types)


    // Extracting statistics
    val rowsRDD = dataset.select("Length","Height").rdd
    val stats = StatisticsUtil.extractStat(rowsRDD)

    // Remove null values
    val schema = StructType(Array(StructField("Length",FloatType,true),StructField("Height",FloatType,true)))
    val dNew = sqlContext.createDataFrame(rowsRDD,schema).na.drop()

    // OUTLIERS USING Z-SCORES
    // Extracting statistics on Length
    val lengthRDD = dataset.map(row=>row.getFloat(1).toDouble)
    val lengthStats = lengthRDD.stats()
    val mean = lengthStats.mean
    val stdev = lengthStats.stdev

    // Identifying outliers with z-Scores
    val zScores = lengthRDD.map(value=> (value-mean)/stdev)
    val zOutliers = zScores.filter(value => ((-3.0d > value)||(value > 3.0d)))
    zOutliers.collect()

    // Display outliers
    zOutliers.foreach(println)

    // OUTLIERS USING INTERQUARTILE RANGE

    // Getting the array of values for Length
    val lengthArray = dataset.select("Length").sort("Length").map(row=>row.getFloat(0).asInstanceOf[Double]).collect()

    // Retrieve the first and third percentile
    val q1Idx:Int = (lengthArray.size*0.25).toInt
    val q3Idx:Int = (lengthArray.size*0.75).toInt

    val Q1 = lengthArray(q1Idx)
    val Q3 = lengthArray(q3Idx)
    val IQR = Q3-Q1
    val lower = Q1-1.5d*IQR
    val upper = Q3+1.5d*IQR

    // Identify and display outliers
    val iqrOutliers = dataset.select("Length").rdd.filter(row=> isOutlier(row,lower,upper)).collect()
    println("Outlier Detection (IQR):")
    println("  Q1: "+Q1)
    println("  Q3: "+Q3)
    println("  IQR: "+IQR)
    println("  LowerBound: "+lower)
    println("  UpperBound: "+upper)
    iqrOutliers.foreach(println)
  }
}
