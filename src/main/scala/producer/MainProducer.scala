package producer

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Point d'entrée principal de l'application Producer
 * Responsable de l'orchestration du processus de streaming des données de taxis
 */
object MainProducer {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Charger la configuration
    val config = ConfigFactory.load()
    val taxiConfig = config.getConfig("taxi.producer")
    
    // Extraire les paramètres de configuration
    val sourceFile = taxiConfig.getConfig("data").getString("sourceFile")
    val batchSize = taxiConfig.getConfig("data").getInt("batchSize")
    val intervalSeconds = taxiConfig.getConfig("data").getInt("intervalSeconds")
    
    logger.info(s"Initializing Yellow Taxi Trip Producer with source: $sourceFile, " +
                s"batch size: $batchSize, interval: $intervalSeconds seconds")
    
    // Créer la session Spark avec configuration optimisée pour mémoire
    val spark = SparkSession.builder()
      .appName("YellowTaxiTripProducer")
      .master("local[*]")
      .config("spark.ui.enabled", "false")  // Désactiver l'UI Spark
      .config("spark.sql.shuffle.partitions", "10")  // Réduire les partitions pour éviter trop de mémoire
      .config("spark.memory.fraction", "0.7")  // Donner plus de mémoire à Spark
      .config("spark.memory.storageFraction", "0.2")  // Réduire mémoire pour storage
      .getOrCreate()
    
    try {
      // Initialiser les composants
      val dataOps = new ProducerOperations(spark, taxiConfig)
      val kafkaSender = new KafkaSender(taxiConfig.getConfig("kafka"))
      
      // Charger et trier les données - mais en batches pour économiser la mémoire
      logger.info("Processing data in batches to save memory")
      
      // Définir le schéma manuellement pour éviter d'inférer le schéma (économise de la mémoire)
      val df = dataOps.loadTripDataWithoutSorting()
      
      // Trier les données dans Spark
      val sortedDf = dataOps.sortDataFrame(df)
      
      // Partitionner en lots plus petits
      val batches = dataOps.createDataBatches(sortedDf, batchSize)
      
      logger.info(s"Created ${batches.length} batches, beginning transmission")
      
      // Traiter chaque lot avec délai
      val processingFuture = Future {
        var counter = 0
        batches.foreach { batch =>
          counter += 1
          logger.info(s"Processing batch $counter of ${batches.length}")
          
          // Convertir le batch en JSON
          val jsonMessages = dataOps.convertBatchToJson(batch)
          
          // Envoyer à Kafka
          kafkaSender.sendJsonMessages(jsonMessages)
          
          // Nettoyage explicite pour libérer la mémoire
          batch.unpersist()
          
          // Attendre l'intervalle configuré avant le prochain lot
          if (counter < batches.length) {
            logger.info(s"Waiting $intervalSeconds seconds before next batch")
            Thread.sleep(intervalSeconds * 1000)
          }
        }
      }
      
      // Attendre la fin du traitement
      Await.result(processingFuture, Duration.Inf)
      
      // Nettoyage
      kafkaSender.closeConnection()
      logger.info("Data streaming completed successfully")
      
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during execution: ${e.getMessage}", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}