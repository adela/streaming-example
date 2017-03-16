package com.bigdata.streaming.example.driver

import com.bigdata.streaming.example.domain.Location
import com.bigdata.streaming.example.util.{Config, ModelUtil}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingDriver {

  val sparkSession = getSparkSession()
  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val modelPath = if (args.size>0) args(0) else Config.ModelOutput
    val model = KMeansModel.load(modelPath)

    val inputDStream = createInputDStream(ssc)

    handleInputStream(inputDStream, model)

    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(secondsInterval:Int, sc: SparkContext): StreamingContext = {
    new StreamingContext(sc, Seconds(secondsInterval))
  }

  def createInputDStream(ssc: StreamingContext) : DStream[String] = {
    val groupId = "testUberGroup"
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.BrokersList ,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true")

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Set(Config.Topic), kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      consumerStrategy).map(_.value())
    messagesDStream
  }

  def handleInputStream(inputStream: DStream[String], model: KMeansModel) : Unit = {
    inputStream.foreachRDD { rdd =>
      if (rdd.count()>0) {
        import sparkSession.implicits._
        val df = rdd.map(parseLocation).toDF()
        val predictionsDF = ModelUtil.addPredictions(df, model)
        predictionsDF.show()
        predictionsDF.createOrReplaceTempView("locations")
        sparkSession.sql("select prediction, count(prediction) as count from locations group by prediction ").show()
      }
    }
  }

  def getSparkSession(): SparkSession = {
    val defaultMaster = "local[*]"
    val conf = new SparkConf()
    conf.set("spark.streaming.kafka.maxRatePerPartition", "20000")
    conf.set("spark.driver.allowMultipleContexts","true")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .master(conf.get("spark.master", defaultMaster))
      .getOrCreate()
    spark
  }

  def parseLocation(str: String): Location = {
    val parts = str.split(",")
    Location(parts(0), parts(1).toDouble, parts(2).toDouble, parts(3))
  }
}
