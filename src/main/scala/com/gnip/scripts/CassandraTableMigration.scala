package com.gnip.scripts

import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

object CassandraTableMigration {

  case class newDataFrame(year: Short, month: Short, date: Short, timestamp : Long, id: Long, id_str: String,
                          created_at: String, tweet_handle: String, text: String,
                          favourite_count: Short, retweet_count: Short, entities: String, user: String,
                          raw_tweet: String)

  /**
    * This is used to migrate data from one cassandra table to another by adding or modifying some keys.
    * This helps in cases when there is some discrepancy in data due to some bug.
    * It will read data from existing table and do some processing and write it to a new table.
    * @param args
    */
  def main(args: Array[String]) {
    val appConfig = ConfigFactory.load()

    val conf = new SparkConf().setAppName("cassandraTableMigration").setMaster(appConfig.getString("master"))
      .set("spark.cassandra.connection.host", appConfig.getString("cassandra.host"))
    val sc = new SparkContext(conf)

    val meta = sc.cassandraTable(appConfig.getString("cassandra.keyspace"), appConfig.getString("cassandra.table"))

    meta.
      filter { f =>

      implicit val formats = DefaultFormats
      val rawTweet = f.getString("raw_tweet")
      //filter tweets where they are replies and not user's tweets or re-tweets
      (parse(rawTweet) \ "in_reply_to_status_id").extractOpt.isEmpty

    }.
      map { r =>

      implicit val formats = DefaultFormats
      val JSON = parse(r.getString("raw_tweet"))

      new newDataFrame(r.getShort("year"), r.getShort("month"), r.getShort("date"),
        (JSON\"timestamp_ms").extract[String].toLong,
        r.getLong("id"), r.getString("id_str"), r.getString("created_at"), r.getString("tweet_handle"), r.getString("text"), 0, 0,
        r.getString("entities" ), r.getString("user" ), r.getString("raw_tweet"))
    }.saveToCassandra("", "")//give the keyspace and table where data is to be migrated
  }
}
