name := "NYC-Taxi-Spark-Streaming"
version := "1.0"
scalaVersion := "2.12.15"

// Versions communes
val sparkVersion = "3.3.2"
val kafkaVersion = "3.3.1"
val configVersion = "1.4.2"

// Dépendances communes pour tous les composants
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe" % "config" % configVersion
)

// Sous-projet Producer
lazy val producer = (project in file("producer"))
  .settings(
    name := "nyc-taxi-producer",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
    )
  )

// Sous-projet Consumer
lazy val consumer = (project in file("consumer"))
  .settings(
    name := "nyc-taxi-consumer",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )

// Sous-projet ML
lazy val ml = (project in file("ml"))
  .settings(
    name := "nyc-taxi-ml",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )

// Projet principal qui agrège tous les sous-projets
lazy val root = (project in file("."))
  .aggregate(producer, consumer, ml)