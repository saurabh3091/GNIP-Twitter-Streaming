package com.gnip.KafkaConsumer

import com.gnip.KafkaConsumer.DatabaseConnectors.mongodb
import com.gnip.KafkaConsumer.DatabaseConnectors.cassandra
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaConsumer {
  /**
    * This job works as Kafka Consumer to read data from kafka and saves data to mongo and cassandra.
 *
    * @param args
    */
  def main(args: Array[String]) {
    val appConfig = ConfigFactory.load()
    val sparkConf = new SparkConf().setAppName("KafkaConsumer").setMaster(appConfig.getString("master"))
      .set("spark.cassandra.connection.host", appConfig.getString("cassandra.host"))
      .set("spark.cassandra.connection.keep_alive_ms", appConfig.getString("cassandra.connectionKeepAliveTime"))

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val readTweets = {
      val kafkaParams = Map[String, String]("metadata.broker.list" -> appConfig.getString("kafka.broker"),
        "zookeeper.connect" -> appConfig.getString("zookeeper"))

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,
        Set(appConfig.getString("kafka.topic")))
    }

    readTweets.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    ///save the tweets to mongo
    mongodb.saveToMongo(readTweets)

    //save the tweets to cassandra
    cassandra.saveToCassandraDB(ssc, readTweets)

    readTweets.foreachRDD(_.unpersist())

    ssc.start()
    ssc.awaitTermination()

  }
}

