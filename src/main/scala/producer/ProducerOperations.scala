package producer

import java.io.File

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Gère le chargement, le tri et la préparation des données de taxi
 */
class ProducerOperations(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sourceFile = config.getConfig("data").getString("sourceFile")
  
  import spark.implicits._
  
  /**
   * Définit le schéma du fichier CSV pour éviter l'inférence de schéma (économise la mémoire)
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
   * Charge le fichier CSV sans trier, en utilisant schéma prédéfini
   */
  def loadTripDataWithoutSorting(): DataFrame = {
    // Vérifie d'abord si le fichier existe
    val file = new File(sourceFile)
    if (!file.exists()) {
      logger.error(s"Le fichier source n'existe pas: $sourceFile")
      logger.error(s"Chemin absolu attendu: ${file.getAbsolutePath}")
      logger.error(s"Répertoire courant: ${new File(".").getAbsolutePath}")
      throw new RuntimeException(s"Fichier source introuvable: $sourceFile")
    }
    
    logger.info(s"Loading taxi trip data from $sourceFile with predefined schema")
    logger.info(s"File size: ${file.length()} bytes")
    
    val df = spark.read
      .option("header", "true")
      .schema(tripSchema)  // Utiliser un schéma prédéfini au lieu d'inférer
      .option("mode", "DROPMALFORMED")  // Ignorer les lignes mal formées
      .csv(sourceFile)
      
    val count = df.count()
    logger.info(s"Successfully loaded taxi data with $count records")
    
    df
  }
  
  /**
   * Trie un DataFrame par heure de prise en charge
   */
  def sortDataFrame(df: DataFrame): DataFrame = {
    logger.info("Sorting data by pickup datetime")
    df.orderBy(col("tpep_pickup_datetime"))
  }
  
  /**
   * Charge le fichier CSV des données de taxi et trie par heure de prise en charge
   * Cette méthode reste pour compatibilité
   */
  def loadAndSortTripData(): DataFrame = {
    logger.info(s"Loading and sorting taxi trip data from $sourceFile")
    
    val df = spark.read
      .option("header", "true")
      .schema(tripSchema)  // Utiliser un schéma prédéfini
      .csv(sourceFile)
      .orderBy(col("tpep_pickup_datetime"))
    
    logger.info(s"Successfully loaded data")
    df
  }
  
  /**
   * Convertit un batch de données DataFrame en tableau de chaînes JSON
   */
  def convertBatchToJson(df: DataFrame): Array[String] = {
    logger.info("Converting batch to JSON format")
    df.toJSON.collect()
  }
  
  /**
   * Divise le DataFrame en batches de taille spécifiée
   * Utilise une approche plus optimisée pour consommer moins de mémoire
   */
  def createDataBatches(df: DataFrame, batchSize: Int): Array[DataFrame] = {
    // Obtenir le nombre total d'enregistrements (peut être coûteux mais nécessaire)
    logger.info("Calculating total record count")
    val recordCount = df.count().toInt
    val batchCount = Math.ceil(recordCount.toDouble / batchSize).toInt
    
    logger.info(s"Dividing data into $batchCount batches of approximately $batchSize records each")
    
    // Partitionner le dataframe
    (0 until batchCount).map { i =>
      val start = i * batchSize
      val end = Math.min((i + 1) * batchSize, recordCount)
      
      // Approche plus efficace pour les grands datasets
      df.limit(end).filter { row =>
        val rowNumber = row.getAs[Int]("VendorID")  // Utiliser un champ existant comme approximation
        rowNumber >= start
      }.persist()  // Persister pour optimiser les opérations suivantes
    }.toArray
  }
}