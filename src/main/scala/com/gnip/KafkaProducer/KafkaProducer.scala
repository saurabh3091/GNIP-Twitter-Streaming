package com.gnip.KafkaProducer

import java.util.HashMap

import com.gnip.KafkaProducer.Connectors.GnipUtils
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaProducer {
  /**
    * This job works to establish connections and retrieve data from GNIP through a streaming connection.
    * This data is then pushed to kafka for consumption by Kafka Consumer job.
 *
    * @param args
    */
  def main(args: Array[String]) {

    val appConfig = ConfigFactory.load()

    val conf = new SparkConf().setAppName("KafkaProducer").setMaster(appConfig.getString("master"))
    val ssc = new StreamingContext(conf, Seconds(10))

    val gnipDstream = {
      if (appConfig.getString("gnip.streamType").equalsIgnoreCase("replay")) {
        GnipUtils.createStream(ssc,
          "https://gnip-stream.gnip.com/replay/powertrack/accounts/SixSigmaAcademy/publishers/twitter/prod.json?" +
            "fromDate=" + appConfig.getString("gnip.replay.fromDate") + "&toDate=" + appConfig.getString("gnip.replay.toDate"),
          appConfig.getString("gnip.id"), appConfig.getString("gnip.pass"))
      }
      else {
        GnipUtils.createStream(ssc,
          "https://gnip-stream.twitter.com/stream/powertrack/accounts/SixSigmaAcademy/publishers/twitter/prod.json" +
            "?backfillMinutes=5’",
          appConfig.getString("gnip.id"), appConfig.getString("gnip.pass"))
      }
    }

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getString("kafka.broker"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink(props))
    sendToKafka(gnipDstream, appConfig.getString("kafka.topic"), kafkaSink)
    ssc.start()
    ssc.awaitTermination()
  }

  def sendToKafka(tweetStream: DStream[String], kafkaTop: String, kafkaSink: Broadcast[KafkaSink]): Unit = {
    tweetStream.foreachRDD { rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(kafkaTop, message)
      }
    }
  }
}



