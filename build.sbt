name := "GNIP-Twitter-Extraction"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided"
libraryDependencies += "org.json4s" % "json4s-native_2.10" % "3.2.10"
libraryDependencies += "com.stratio.datasource" % "spark-mongodb_2.10" % "0.11.2"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

