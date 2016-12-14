# GNIP-Twitter-Streaming
Scala-Spark-Kafka-Cassandra-Mongo
Just edit GNIP-Twitter-Streaming/src/main/resources/application.conf
compile and build using sbt assembly
run using spark-submit --class com.gnip.KafkaConsumer.KafkaConsumer --jar path-to-jar
or spark-submit --class com.gnip.KafkaConsumer.KafkaConsumer --jar path-to-jar

Also contains scripts to migrate data from one cassandra table to another.
