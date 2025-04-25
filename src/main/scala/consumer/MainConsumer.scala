package consumer

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Point d'entrée principal pour le Consumer Spark Streaming
 * Responsable de l'initialisation et l'orchestration du traitement en streaming
 */
object MainConsumer {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    logger.info("Initializing Yellow Taxi Trip Consumer")
    
    // Charger la configuration
    val config = ConfigFactory.load()
    val consumerConfig = config.getConfig("taxi.consumer")
    val sparkConfig = consumerConfig.getConfig("spark")
    
    // Paramètres Spark
    val appName = sparkConfig.getString("appName")
    val batchInterval = sparkConfig.getInt("batchIntervalSeconds")
    val shufflePartitions = sparkConfig.getInt("shufflePartitions")
    
    logger.info(s"Creating Spark session with app name: $appName, " +
                s"batch interval: $batchInterval seconds, " +
                s"shuffle partitions: $shufflePartitions")
    
    // Créer la session Spark
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()
    
    // Définir le niveau de log pour éviter trop de verbosité
    spark.sparkContext.setLogLevel("WARN")
    
    try {
      // Initialiser les composants
      val fileReader = new FileReader(spark, consumerConfig)
      val streamProcessor = new StreamProcessor(spark, consumerConfig)
      
      // Créer le stream depuis les fichiers
      val inputStream = fileReader.createFileStream()
      
      // Démarrer le traitement du stream
      val query = streamProcessor.processStream(inputStream)
      
      // Attendre la fin du stream (ne terminera pas normalement)
      logger.info("Waiting for streaming to finish...")
      query.awaitTermination()
      
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during consumer execution: ${e.getMessage}", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}