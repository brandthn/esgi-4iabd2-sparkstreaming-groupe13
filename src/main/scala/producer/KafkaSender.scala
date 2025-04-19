package producer

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * Responsable de l'envoi des messages au broker Kafka
 */
class KafkaSender(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private val topic = config.getString("topic")
  private val brokers = config.getString("brokers")
  
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
   * Envoie un lot de messages JSON au topic Kafka
   */
  def sendJsonMessages(jsonMessages: Array[String]): Unit = {
    logger.info(s"Sending batch of ${jsonMessages.length} messages to topic $topic")
    
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
    logger.info(s"Batch transmission complete. Successful: $successCount, Failed: $failureCount")
  }
  
  /**
   * Ferme proprement la connexion au producteur Kafka
   */
  def closeConnection(): Unit = {
    logger.info("Closing Kafka producer connection")
    producer.close()
  }
}