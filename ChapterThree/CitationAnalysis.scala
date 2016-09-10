package ch03

import org.apache.spark.{SparkConf, SparkContext}
import scala.xml._
/**
  * Created by Giancarlo on 6/7/2016.
  */
object CitationAnalysis {
  def getRecordsFromXML(xmlstring: String, tag: String): Iterator[Node] = {
    val nodes = XML.loadString(xmlstring) \\ tag
    nodes.toIterator
  }

  def getValue(xml: String, tag: String): String = {
    (XML.loadString(xml) \\ tag).text
  }

  def getName(node: Node): String = {
    (node \ "name" \ "given-names").text + " " + (node \ "name" \ "surname").text
  }

  def getAuthors(nodes: Iterator[Node]): String = {
    val buffer = new StringBuilder()
    nodes.foreach(e=>buffer.append(getName(e)).append(", "))
    return buffer.toString().substring(0,buffer.toString().length-2)
  }

  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Citation Analysis")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf);

    val papers = sc.wholeTextFiles("c:/Spark/data/03-IntroductionToSpark/ScientificPapers_XML")
    val records = papers.map(e=>(getValue(e._2,"article-id"),getAuthors(getRecordsFromXML(e._2,"contrib"))))
    val aCount = records.flatMap(e=>e._2.split(",")).map(e=>(e.trim(),1)).reduceByKey((v1,v2)=>v1+v2)

    aCount.foreach(println)
  }
}