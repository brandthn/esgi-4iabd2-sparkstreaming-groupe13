package consumer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * FileReader est responsable de lire les données des fichiers JSON 
 * générés par le Producer et de créer un DataFrame pour le traitement en streaming
 */
class FileReader(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Configurations de la source de données
  private val sourceConfig = config.getConfig("source")
  private val sourceType = sourceConfig.getString("type")
  private val directory = sourceConfig.getString("directory")
  private val format = sourceConfig.getString("format")
  
  /**
   * Définition du schéma pour les messages JSON
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
   * Crée un DataFrame de streaming à partir des fichiers du répertoire
   */
  def createFileStream(): DataFrame = {
    logger.info(s"Creating file stream from directory: $directory with format: $format")
    
    // Vérifier que le type de source est bien "file"
    if (sourceType != "file") {
      logger.warn(s"Source type is not 'file' but '$sourceType'. Proceeding with file stream anyway.")
    }
    
    // Lire les données des fichiers JSON
    val fileStream = spark.readStream
      .option("maxFilesPerTrigger", 1) // Traiter un fichier à la fois pour simuler un flux
      .schema(tripSchema) // Appliquer le schéma prédéfini
      .format(format)
      .load(directory)
    
    logger.info("File stream created successfully")
    fileStream
  }
}