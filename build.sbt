name := "projet-spark-streaming"
version := "0.1.0"
scalaVersion := "2.12.15"

// Dépendances du projet
libraryDependencies ++= Seq(
  // Apache Spark
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  
  // Spark Streaming - AJOUTEZ CES DEUX LIGNES
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  
  // Kafka
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  
  // Configuration
  "com.typesafe" % "config" % "1.4.2",
  
  // JSON4S (toutes les dépendances pour éviter les ClassNotFoundException)
  "org.json4s" %% "json4s-core" % "3.7.0-M11",
  "org.json4s" %% "json4s-ast" % "3.7.0-M11",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "org.json4s" %% "json4s-native" % "3.7.0-M11",
  
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.2.10"
)