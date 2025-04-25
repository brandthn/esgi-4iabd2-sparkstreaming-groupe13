package consumer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * KafkaReader est responsable de lire les données de Kafka
 * et de créer un DataFrame pour le traitement en streaming
 */
class KafkaReader(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Configurations Kafka
  private val kafkaConfig = config.getConfig("kafka")
  private val topic = kafkaConfig.getString("topic")
  private val brokers = kafkaConfig.getString("brokers")
  private val groupId = kafkaConfig.getString("groupId")
  
  /**
   * Définition du schéma pour les messages JSON reçus de Kafka
   * Ce schéma doit correspondre exactement à celui défini dans le Producer
   */
  private val tripSchema = StructType(Array(
    StructField("VendorID", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", DoubleType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("RatecodeID", DoubleType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("PULocationID", IntegerType, true),
    StructField("DOLocationID", IntegerType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("congestion_surcharge", DoubleType, true),
    StructField("Airport_fee", DoubleType, true)
  ))
  
  /**
   * Crée un DataFrame de streaming à partir de Kafka
   */
  def createKafkaStream(): DataFrame = {
    logger.info(s"Creating Kafka stream for topic $topic from brokers at $brokers")
    
    // Lire les données de Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest") // Commencer à partir des derniers offsets
      .option("kafka.group.id", groupId)
      .option("failOnDataLoss", "false") // Ne pas échouer si des données sont perdues
      .load()
    
    // Extraire la valeur des messages comme chaîne
    val valueStream = kafkaStream.selectExpr("CAST(value AS STRING)")
    
    // Analyser le JSON en utilisant le schéma prédéfini
    val parsedStream = valueStream
      .select(from_json(col("value"), tripSchema).as("trip"))
      .select("trip.*")
    
    logger.info("Kafka stream created and JSON data parsed successfully")
    parsedStream
  }
}