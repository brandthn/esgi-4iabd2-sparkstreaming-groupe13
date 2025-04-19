// src/main/scala/com/taxi/producer/TaxiProducerApp.scala
package com.taxi.producer

import org.apache.spark.sql.SparkSession

object TaxiProducerApp {
  
  def main(args: Array[String]): Unit = {
    // Configuration
    val bootstrapServers = Config.Kafka.bootstrapServers
    val topic = Config.Kafka.topic
    val dataPath = Config.Taxi.dataPath
    val speedFactor = Config.Taxi.Simulation.speedFactor
    val batchSize = Config.Taxi.Simulation.batchSize
    
    println("NYC Taxi Data Producer")
    println(s"Kafka Bootstrap Servers: $bootstrapServers")
    println(s"Kafka Topic: $topic")
    println(s"Data Path: $dataPath")
    println(s"Speed Factor: $speedFactor")
    println(s"Batch Size: $batchSize")
    
    // Initialiser SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Producer")
      .master("local[*]")
      .getOrCreate()
    
    
    try {
      // Initialiser le Producer Kafka
      val kafkaProducer = new KafkaProducerWrapper(bootstrapServers)
      
      // Initialiser le simulateur de données
      val simulator = new DataSimulator(spark, dataPath, speedFactor, batchSize)
      
      // Charger les données
      println("Loading taxi data...")
      val taxiDF = simulator.loadData()
      println(s"Loaded ${taxiDF.count()} records")
      
      // Simuler le flux de données
      println("Starting data simulation...")
      simulator.simulateDataFlow(taxiDF, kafkaProducer, topic)
      
      // Fermer le Producer
      kafkaProducer.close()
      
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Arrêter SparkSession
      spark.stop()
    }
  }
}