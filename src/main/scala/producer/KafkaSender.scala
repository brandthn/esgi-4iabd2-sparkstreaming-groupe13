package producer

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties
import java.io.{File, PrintWriter}
import scala.util.{Failure, Success, Try}

/**
 * Responsable de l'envoi des messages au broker Kafka
 */
class KafkaSender(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private val topic = config.getString("topic")
  private val brokers = config.getString("brokers")
  
  // Configuration pour déboguer les batchs (ajout)
  private val debugEnabled = if (config.hasPath("debugOutput.enabled")) 
    config.getBoolean("debugOutput.enabled") else false
  private val debugOutputFile = if (debugEnabled && config.hasPath("debugOutput.file")) 
    Some(config.getString("debugOutput.file")) else None
  private val debugOutputConsole = if (debugEnabled && config.hasPath("debugOutput.console")) 
    config.getBoolean("debugOutput.console") else false
  
  private val producer = createKafkaProducer()
  
  /**
   * Initialise et configure le producteur Kafka
   */
  private def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    
    // Configurations supplémentaires pour améliorer la performance et la fiabilité
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432") // 32MB
    
    logger.info(s"Creating Kafka producer connection to brokers: $brokers for topic: $topic")
    new KafkaProducer[String, String](props)
  }
  
  /**
   * Enregistre le contenu du batch pour déboguer (ajout)
   */
  private def debugBatch(batchId: Int, jsonMessages: Array[String]): Unit = {
    if (debugEnabled) {
      // Préparation du contenu à écrire
      val batchHeader = s"===== BATCH #$batchId (${jsonMessages.length} messages) ====="
      val batchContent = if (jsonMessages.nonEmpty) {
        jsonMessages.take(5).mkString("\n").take(1000) + 
        (if (jsonMessages.length > 5) "\n... (plus de données)" else "")
      } else {
        "BATCH VIDE - Aucun message à envoyer!"
      }
      val batchFooter = "======================================"
      val fullContent = s"$batchHeader\n$batchContent\n$batchFooter\n\n"
      
      // Écrire dans la console si configuré
      if (debugOutputConsole) {
        println("\n" + fullContent)
      }
      
      // Écrire dans un fichier si configuré
      debugOutputFile.foreach { filePath =>
        try {
          val file = new File(filePath)
          // Crée le répertoire parent si nécessaire
          file.getParentFile.mkdirs()
          
          val writer = new PrintWriter(new java.io.FileWriter(file, true))
          try {
            writer.println(fullContent)
          } finally {
            writer.close()
          }
        } catch {
          case e: Exception => logger.error(s"Failed to write debug output to file: ${e.getMessage}", e)
        }
      }
    }
  }
  
  // Compteur de batches pour le débogage (ajout)
  private var batchCounter = 0
  
  /**
   * Envoie un lot de messages JSON au topic Kafka
   */
  def sendJsonMessages(jsonMessages: Array[String]): Unit = {
    batchCounter += 1
    logger.info(s"Sending batch #$batchCounter of ${jsonMessages.length} messages to topic $topic")
    
    // Déboguer le batch avant l'envoi (ajout)
    debugBatch(batchCounter, jsonMessages)
    
    var successCount = 0
    var failureCount = 0
    
    jsonMessages.zipWithIndex.foreach { case (json, idx) =>
      // Utiliser l'index comme clé pour assurer la distribution
      val record = new ProducerRecord[String, String](topic, idx.toString, json)
      
      Try(producer.send(record).get()) match {
        case Success(_) => 
          successCount += 1
          if (successCount % 10 == 0) {
            logger.debug(s"Successfully sent $successCount messages so far")
          }
          
        case Failure(e) => 
          failureCount += 1
          logger.error(s"Failed to send message $idx: ${e.getMessage}", e)
      }
    }
    
    producer.flush()
    logger.info(s"Batch #$batchCounter transmission complete. Successful: $successCount, Failed: $failureCount")
  }
  

  def closeConnection(): Unit = {
    logger.info("Closing Kafka producer connection")
    producer.close()
  }
}