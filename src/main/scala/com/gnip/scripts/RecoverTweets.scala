package com.gnip.scripts

import com.datastax.spark.connector.streaming._
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

object RecoverTweets {
  val appConfig = ConfigFactory.load()

  /**
    * This helps in recovering tweets from kafka in cases where Consumer fails due to some or other issue.
    * We set the kafka to read all the data present inside a topic and push it to cassandra. It wont affect the data
    * that is already pushed to cassandra as the primary key will be same and data will be overwritten without creating
    * any duplicates.
    * @param args
    */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("RecoverTweets").setMaster(appConfig.getString("master")).
      set("spark.cassandra.connection.host", appConfig.getString("cassandra.host"))
      .set("spark.cassandra.connection.keep_alive_ms", appConfig.getString("cassandra.connectionKeepAliveTime"))

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val readTweets = {
      //offset is set to "smallest" to fetch all tweets from kafka
      val kafkaParams = Map[String, String]("metadata.broker.list" -> appConfig.getString("kafka.broker"),
        "zookeeper.connect" -> appConfig.getString("zookeeper"), "auto.offset.reset" -> "smallest")

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,
        Set(appConfig.getString("kafka.topic")))
    }

    readTweets.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    readTweets.foreachRDD(t => println("without filter" + t.count()))
    readTweets.filter(t => filterTweetsOnDate(t._2)).foreachRDD(t => println("with filter" + t.count()))

    val filteredTweets = readTweets.filter(t => filterTweetsOnDate(t._2))

    //save the tweets to cassandra
    saveToCassandra(ssc, filteredTweets)

    readTweets.foreachRDD(_.unpersist())

    ssc.start()
    ssc.awaitTermination()
  }

  def filterTweetsOnDate(tweet: String): Boolean = {
    implicit val formats = DefaultFormats
    tweet.contains("timestamp_ms") && (parse(tweet) \ "timestamp_ms").extract[String].toLong > 1478716200000L &&
      (parse(tweet) \ "timestamp_ms").extract[String].toLong < 1479282255000L
  }

  def saveToCassandra(ssc: StreamingContext, filteredTweets: DStream[(String, String)]): Unit = {

    filteredTweets.filter { t =>
      implicit val formats = DefaultFormats
      //filter tweets where they are replies and not user's tweets or re-tweets
      (parse(t._2) \ "in_reply_to_status_id").extractOpt[String].isEmpty
    }.map { t =>
      implicit val formats = DefaultFormats
      val JSON = parse(t._2)
      val tweetHandle = (JSON \ "user" \ "screen_name").extract[String]

      val date = new DateTime((JSON \ "timestamp_ms").extract[String].toLong, DateTimeZone.UTC)

      new twitterWithMeta(date.getYear.toShort, date.getMonthOfYear.toShort, date.getDayOfMonth.toShort, date.getHourOfDay.toShort,
        (JSON \ "id").extract[String].toLong, (JSON \ "id").extract[String], (JSON \ "created_at").extract[String],
        tweetHandle, (JSON \ "text").extract[String], 0, 0, compact(render(JSON \ "entities")), compact(render(JSON \ "user")),
        t._2)
    }.saveToCassandra(appConfig.getString("cassandra.keyspace"), "")//new table where data is to be pushed
  }

  case class twitterWithMeta(year: Short, month: Short, date: Short, hour: Short, id: Long, id_str: String,
                             created_at: String, tweet_handle: String, text: String,
                             favourite_count: Short, retweet_count: Short, entities: String, user: String,
                             raw_tweet: String)

}


