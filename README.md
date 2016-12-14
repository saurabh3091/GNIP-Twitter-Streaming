# GNIP-Twitter-Streaming
Scala-Spark-Kafka-Cassandra-Mongo

Just edit GNIP-Twitter-Streaming/src/main/resources/application.conf 

and define configurations for Kafka, Mongo, Cassandra, ZooKeeper in application.conf

Compile and build using "sbt assembly"

Run using spark-submit --class com.gnip.KafkaConsumer.KafkaConsumer --jar path-to-jar

or spark-submit --class com.gnip.KafkaProducer.KafkaProducer --jar path-to-jar

Also contains scripts to migrate data from one cassandra table to another.
