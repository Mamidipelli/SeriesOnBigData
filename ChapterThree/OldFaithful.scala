package ch03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Giancarlo on 9/10/2016.
  * Make sure to change the path to the dataset
  */
object OldFaithful {
  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Old Faithful")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)
    val dataset = sc.textFile("C:\\Spark\\data\\03-IntroductionToSpark\\OldFaithfulGeyserDataset.csv").map(line=>line.split(",")).map(elem=>(elem(0).toFloat,elem(1).toFloat))
    dataset.top(2).foreach (println)

    case class EruptionItem (duration:Float, waiting:Float)
    val dataset2 = sc.textFile("C:\\Spark\\data\\03-IntroductionToSpark\\OldFaithfulGeyserDataset.csv")
      .map(line=>line.split(","))
      .map(elem=>EruptionItem(elem(0).toFloat,elem(1).toFloat))

    dataset2.take(5).foreach(e=>print(e.duration))
  }
}
