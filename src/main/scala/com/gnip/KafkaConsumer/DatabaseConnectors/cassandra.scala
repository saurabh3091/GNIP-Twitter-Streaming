package com.gnip.KafkaConsumer.DatabaseConnectors

import com.datastax.spark.connector.streaming._
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

object cassandra {
  val appConfig = ConfigFactory.load()

  /**This function filters and saves the tweets to cassandra.
    *
    * @param ssc streaming context
    * @param readTweets stream of tweets read from kafka
    */
  def saveToCassandraDB(ssc: StreamingContext, readTweets: InputDStream[(String, String)]) = {

    ///save the tweets to cassandra
    readTweets.filter { t =>
      implicit val formats = DefaultFormats
      //filter tweets where they are replies and not user's tweets or re-tweets
      (parse(t._2) \ "in_reply_to_status_id").extractOpt[String].isEmpty
    }.map { t =>
      implicit val formats = DefaultFormats

      val JSON = parse(t._2)
      val tweetHandle = (JSON \ "user" \ "screen_name").extract[String]
      val date = new DateTime((JSON \ "timestamp_ms").extract[String].toLong, DateTimeZone.UTC)

      new twitterWithMeta(date.getYear.toShort, date.getMonthOfYear.toShort, date.getDayOfMonth.toShort, date.getHourOfDay.toShort,
        (JSON \ "id").extract[String].toLong, (JSON \ "id").extract[String], (JSON \ "created_at").extract[String], tweetHandle, (JSON \ "text").extract[String], 0, 0, compact(render(JSON \ "entities")), compact(render(JSON \ "user")),
        t._2)

    }.saveToCassandra(appConfig.getString("cassandra.keyspace"), appConfig.getString("cassandra.table"))
  }

  case class twitterWithMeta(year: Short, month: Short, date: Short, hour: Short, id: Long, id_str: String,
                             created_at: String, tweet_handle: String, text: String,
                             favourite_count: Short, retweet_count: Short, entities: String, user: String,
                             raw_tweet: String)
}
