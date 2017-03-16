package com.bigdata.streaming.producer

import java.util.Properties

import com.bigdata.streaming.example.util.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object PositionsProducer {


  def main(args: Array[String]): Unit = {
    val kafkaProducer = setupKafkaProducer()
    val recordsNum = 10
    for (i<-1 to recordsNum) {
      val message = "2014-08-01 00:00:00,40.7"+generate3Digits()+",-73.9"+generate3Digits()+",B02598"
      println("Sending: "+message)
      kafkaProducer.send(new ProducerRecord(Config.Topic, message))
    }
    kafkaProducer.close()
  }



  def generate3Digits() : Int = {
    val random = new Random()
    random.nextInt(999)
  }

  def setupKafkaProducer() : KafkaProducer[String, String] = {
   new KafkaProducer(getKafkaProp())
  }

  def getKafkaProp(): Properties = {
    val props = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("retries", (0: Integer))
    props.put("batch.size", (16384: Integer))
    props.put("linger.ms", (1: Integer))
    props.put("bootstrap.servers", Config.BrokersList)
    props
  }

}
