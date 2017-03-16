package com.bigdata.streaming.example.driver

import com.bigdata.streaming.example.util.Config
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ClusteringDriver {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession()

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))

    val df = spark.read.option("header","false").schema(schema).csv("data/uber.csv")
    df.show()

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val featureDF = assembler.transform(df)
    featureDF.show()

    val Array(trainingData, testData) = featureDF.randomSplit(Array(0.7, 0.3), 1234)
    val kMeans = new KMeans().setK(8).setFeaturesCol("features").setMaxIter(10)
    val model = kMeans.fit(trainingData)
    model.write.overwrite().save(Config.ModelOutput)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    val predictions = model.transform(testData)
    predictions.show
    predictions.groupBy("prediction").count().show()
    spark.stop()
  }


  def createSparkSession(): SparkSession = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .master(conf.get("spark.master", "local[*]"))
      .getOrCreate()
    spark
  }

}
