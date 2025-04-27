package producer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.io.File

/**
 * ProducerOperation: chargement et la préparation des données de taxi
 */
class ProducerOperations(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sourceFile = config.getConfig("data").getString("sourceFile")
  
  import spark.implicits._
  
  /**
   * Définit schéma du fichier CSV pour éviter l'inférence de schéma (économise ressources et améliore performance de lecture)
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
   * Chargement fichier CSV avec schéma prédéfini et options optimisées
   */
  def loadTripData(): DataFrame = {
    // Vérification si fichier existe
    val file = new File(sourceFile)
    if (!file.exists()) {
      logger.error(s"Le fichier source n'existe pas: $sourceFile")
      logger.error(s"Chemin absolu attendu: ${file.getAbsolutePath}")
      logger.error(s"Répertoire courant: ${new File(".").getAbsolutePath}")
      throw new RuntimeException(s"Fichier source introuvable: $sourceFile")
    }
    
    logger.info(s"Chargement des données depuis $sourceFile")
    logger.info(s"Taille du fichier: ${file.length()} bytes")
    
    // Lire les 5 premières lignes du fichier pour vérifier le format (debug)
    try {
      import scala.io.Source
      val bufferedSource = Source.fromFile(file)
      val lines = bufferedSource.getLines().take(5).toList
      bufferedSource.close()
      
      logger.info("Aperçu des premières lignes du fichier CSV:")
      lines.foreach(line => logger.info(line))
    } catch {
      case e: Exception => logger.warn(s"Impossible de lire l'aperçu du fichier: ${e.getMessage}")
    }
    
    // Options avancées de lecture CSV
    val df = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .option("mode", "PERMISSIVE") // Plus permissif pour les données mal formées
      .option("nullValue", "")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(tripSchema)
      .csv(sourceFile)
    
    // Afficher aperçu du DataFrame
    try {
      val sample = df.limit(2).collect()
      if (sample.nonEmpty) {
        logger.info(s"Aperçu du premier enregistrement: ${sample(0)}")
        logger.info(s"Schéma du DataFrame: ${df.schema}")
      } else {
        logger.warn("Aucun enregistrement trouvé dans le DataFrame")
      }
    } catch {
      case e: Exception => logger.warn(s"Impossible d'afficher l'aperçu: ${e.getMessage}")
    }
    
    val count = df.count()
    logger.info(s"Données chargées avec succès: $count enregistrements")
    
    // Vérifier que le DataFrame contient des données réelles
    val columnCount = df.columns.length
    logger.info(s"Nombre de colonnes: $columnCount (attendu: 19)")
    
    if (count > 0) {
      // Trier les données par heure de prise en charge et retourner le DataFrame
      val sortedDf = df.orderBy(col("tpep_pickup_datetime"))
      
      // Enregistrer quelques exemples pour le débogage
      try {
        val exampleRows = sortedDf.limit(5).collect()
        logger.info(s"Exemples de lignes triées (${exampleRows.length}):")
        exampleRows.foreach(row => logger.info(row.toString()))
      } catch {
        case e: Exception => logger.warn(s"Impossible d'afficher des exemples: ${e.getMessage}")
      }
      
      sortedDf
    } else {
      logger.error("Le DataFrame est vide après chargement!")
      df // Retourner le DataFrame vide quand même
    }
  }
  
  /**
   * Extrait un lot de N lignes à partir de la position spécifiée
   * en utilisant une approche avec row_number
   */
  def extractBatch(df: DataFrame, startIndex: Int, batchSize: Int): DataFrame = {
    logger.info(s"Extraction du batch à partir de l'index $startIndex, taille $batchSize")
    
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    
    // Ajouter un numéro de ligne au df
    val windowSpec = Window.orderBy("tpep_pickup_datetime")
    val dfWithRowNum = df.withColumn("row_num", row_number().over(windowSpec))
    
    // Sélectionner uniquement les lignes dans l'intervalle spécifié
    val result = dfWithRowNum.filter(col("row_num").between(startIndex + 1, startIndex + batchSize))
      .drop("row_num")
    
    // Afficher nombre de lignes pour vérification
    val count = result.count()
    logger.info(s"Batch extrait avec $count lignes")
    
    result
  }

  def convertBatchToJson(df: DataFrame): Array[String] = {
    if (df.isEmpty) {
      logger.warn("DataFrame vide, aucune conversion nécessaire")
      return Array.empty[String]
    }
    
    logger.info(s"Conversion du batch en format JSON")
    try {
      // Action forcée pour voir le nombre d'enregistrements
      val count = df.count()
      logger.info(s"Tentative de conversion de $count enregistrements en JSON")
      
      if (count == 0) {
        return Array.empty[String]
      }
      
      // Échantillon pour vérifier le contenu
      val sample = df.limit(1).collect()
      if (sample.isEmpty) {
        logger.warn("Échantillon vide malgré count > 0, problème possible avec le DataFrame")
        return Array.empty[String]
      }
      
      logger.info(s"Échantillon: ${sample(0).toString}")
      
      // Convertir en JSON
      val jsonArray = df.toJSON.collect()
      logger.info(s"Batch converti avec ${jsonArray.length} messages")
      jsonArray
    } catch {
      case e: Exception =>
        logger.error(s"Erreur lors de la conversion en JSON: ${e.getMessage}", e)
        Array.empty[String]
    }
  }
}