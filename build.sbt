name := "ElasticSearchSimpleProcessing"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/com.sksamuel.elastic4s/elastic4s-core
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "7.1.0"

// https://mvnrepository.com/artifact/com.sksamuel.elastic4s/elastic4s-client-esjava
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "7.1.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"
