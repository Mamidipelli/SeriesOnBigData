package Ch04

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Giancarlo on 9/11/2016.
  */
object SummaryStatistics_EasyWay {
  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Data Preprocessing")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    // Create an RDD of integers
    val list = (1 to 1000000)
    var rdd = sc.parallelize(list)

    println ("Mean: "+rdd.mean())
    println ("Max: "+rdd.max())
    println ("Min: "+rdd.min())
    println ("SD: "+rdd.stdev())
    println ("Variance: "+rdd.variance())
  }
}
