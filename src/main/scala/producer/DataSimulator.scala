// src/main/scala/com/taxi/producer/DataSimulator.scala
package com.taxi.producer

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.util.{Date, Calendar}
import scala.collection.mutable.ArrayBuffer

class DataSimulator(spark: SparkSession, dataPath: String, speedFactor: Int, batchSize: Int) {
  
  import spark.implicits._
  
  def loadData(): DataFrame = {
    val taxiDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataPath)
      .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
      .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
      .orderBy("tpep_pickup_datetime")
    
    taxiDF
  }
  
  // Simuler le flux de données en temps réel avec ajustement de vitesse
  def simulateDataFlow(taxiDF: DataFrame, kafkaProducer: KafkaProducerWrapper, topic: String): Unit = {
    val rows = taxiDF.collect()
    val rowsBuffer = new ArrayBuffer[Row]()
    
    if (rows.isEmpty) {
      println("No data found to process!")
      return
    }
    
    // Obtenez la première horodatage pour référence
    val firstTimestamp = rows(0).getAs[Timestamp]("tpep_pickup_datetime").getTime
    val simulationStartTime = System.currentTimeMillis()
    
    println(s"Starting simulation with speed factor: $speedFactor")
    println(s"Batch size: $batchSize")
    
    for (i <- rows.indices) {
      val row = rows(i)
      val currentTimestamp = row.getAs[Timestamp]("tpep_pickup_datetime").getTime
      
      // Calculer le délai simulé
      val originalDelayMs = currentTimestamp - firstTimestamp
      val simulatedDelayMs = originalDelayMs / speedFactor
      
      // Temps écoulé depuis le début de la simulation
      val elapsedSimulationTime = System.currentTimeMillis() - simulationStartTime
      
      // Si nous sommes en avance sur le calendrier simulé, attendre
      if (simulatedDelayMs > elapsedSimulationTime) {
        val waitTime = simulatedDelayMs - elapsedSimulationTime
        Thread.sleep(waitTime)
      }
      
      // Ajouter la ligne au buffer
      rowsBuffer += row
      
      // Envoyer en batch quand on atteint batchSize ou à la fin
      if (rowsBuffer.size >= batchSize || i == rows.length - 1) {
        sendBatch(rowsBuffer, kafkaProducer, topic)
        rowsBuffer.clear()
      }
      
      // Afficher la progression
      if (i % 1000 == 0) {
        val progress = (i.toDouble / rows.length) * 100
        println(f"Simulation progress: $progress%.2f%%")
      }
    }
    
    println("Simulation completed!")
  }
  
  // Envoyer un lot de données à Kafka
  private def sendBatch(rows: ArrayBuffer[Row], kafkaProducer: KafkaProducerWrapper, topic: String): Unit = {
    println(s"Sending batch of ${rows.size} records to Kafka")
    
    rows.foreach { row =>
      try {
        val taxiRecord = TaxiRecord.fromRow(row)
        val json = TaxiRecord.toJson(taxiRecord)
        
        // Utiliser PULocationID comme clé pour garantir le partitionnement par zone
        val key = taxiRecord.puLocationID.toString
        
        kafkaProducer.send(topic, key, json)
      } catch {
        case e: Exception => 
          println(s"Error processing record: ${e.getMessage}")
      }
    }
    
    // Assurez-vous que les messages sont envoyés
    kafkaProducer.flush()
  }
}