package com.gnip.KafkaConsumer.DatabaseConnectors

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.dstream.InputDStream

object mongodb {

  val appConfig = ConfigFactory.load()

  private val Database      = appConfig.getString("mongo.database")
  private val Collection    = appConfig.getString("mongo.collection")
  private val meta          = appConfig.getString("mongo.meta")
  private val MongoHost     = appConfig.getString("mongo.host")
  private val MongoPort     = appConfig.getInt("mongo.port")

  /**
    * This function saves the tweets read from kafka to mongodb.
    * @param readTweets stream of tweets read from kafka
    */
  def saveToMongo(readTweets: InputDStream[(String, String)]) = {

    val mongoClient = prepareMongoEnvironment()
    val collection = mongoClient(Database)(Collection)
    val twitterHandlesCollection = mongoClient(Database)(meta)

    println("Initialized mongodb connector...")

    readTweets.filter { t =>
      //filter tweets where they are replies and not user's tweets or re-tweets
      com.mongodb.util.JSON.parse(t._2).asInstanceOf[DBObject].get("in_reply_to_status_id") == null
    }.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        println("Reading data from kafka broker... (%s total):".format(count))

        val topList = rdd.take(count.toInt)

        for (tweet <- topList) {
          val doc = com.mongodb.util.JSON.parse(tweet._2).asInstanceOf[DBObject]

          if (collection.find(MongoDBObject("_id" -> doc.get("id"))).count() < 1) {
            doc("tweet_handle") = doc.get("user").asInstanceOf[DBObject].get("screen_name")
            doc("retweet") = doc.get("retweeted")
            doc("_id") = doc.get("id")
            collection.insert(doc)
          }
        }
      }
    })
  }

  private def prepareMongoEnvironment(): MongoClient = {
    val mongoClient = MongoClient(MongoHost, MongoPort)
    mongoClient
  }

  private def closeMongoEnviroment(mongoClient: MongoClient) = {
    mongoClient.close()
    println("mongoclient closed!")
  }

  private def cleanMongoEnvironment(mongoClient: MongoClient) = {
    cleanMongoData(mongoClient)
    mongoClient.close()
  }

  private def cleanMongoData(client: MongoClient): Unit = {
    val collection = client(Database)(Collection)
    collection.dropCollection()
  }
}
