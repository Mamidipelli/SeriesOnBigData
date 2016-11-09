package Ch07

import java.awt.image.BufferedImage
import java.io.{BufferedWriter, File, FileWriter}
import javax.imageio.ImageIO

import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.{PCA, PCAModel, StandardScaler}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Giancarlo on 9/3/2016.
  */
object ExtractImageFeatures {

  // Default size of the image
  var width: Int = 50
  var height: Int = 50
  var vectorSize: Int = width*height
  // Folder containing the greyscale images
  var inputFolder = "C:\\Spark\\data\\07 - Data Mining Tasks\\lfw-deepfunneled_gray\\"

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("PCA")
    .set("spark.executor.memory", "4g")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def getFeatureVector (fPath:String): Array[Double] = {
    val gImage: BufferedImage = ImageIO.read (new File (inputFolder+fPath))
    val result = new Array[Double](vectorSize)
    gImage.getData.getPixels(0,0,width,height,result)
  }

  def main(args: Array[String]) {

    def getFeatureVector (fPath:String): Array[Double] = {
      // 1. Read the image file
      val gImage: BufferedImage = ImageIO.read (new File (inputFolder+fPath))
      // 2. Prepare the array containing the image
      val result = new Array[Double](vectorSize)
      // 3. Store all pixels in the Array of Double
      gImage.getData.getPixels(0,0,width,height,result)
    }

    val images = new File(inputFolder).list()
    val names = images.sorted.map(fileName=>fileName.substring(0,fileName.indexOf("_0"))).toList.distinct

    names.take(10).foreach (name=> println ("["+names.indexOf(name)+"] "+name))

    val fRDD = sc.parallelize(images)

    // Building the RDD of LabeledPoint
    val imgVectors = fRDD.map(fileName=>
                              LabeledPoint(
                                names.indexOf(fileName.substring(0,fileName.indexOf("_0"))),
                                Vectors.dense(getFeatureVector(fileName))
                              )
                             )

    imgVectors.take(5).foreach(v=> {
      v.features.toArray.take(15).foreach (e=>print(e+" "))
      println})

    // Transforming data into dataframesstand
    val imgDF = imgVectors.toDF()

    // Transforming into dense vector
    val stdScaler = new StandardScaler()
    stdScaler.setWithMean(true).setWithStd(true)

    stdScaler.setInputCol("features")
    stdScaler.setOutputCol("stdFeatures")

    // Create the PCA stage
    val pca = new PCA()
    pca.setInputCol(stdScaler.getOutputCol)
    pca.setOutputCol("pcaFeatures")
    pca.setK(15)

    // Create the Pipeline
    val pipeline = new Pipeline()
    pipeline.setStages(Array(stdScaler,pca))

    val pcaModel = pipeline.fit(imgDF)

    // Project the data points to the new linear space identified by the 15 principal components
    val pcaDF = pcaModel.transform(imgDF)
    pcaDF.show(10)

  }
}
