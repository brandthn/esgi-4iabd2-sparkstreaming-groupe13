package producer

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.{File, PrintWriter}
import java.time.LocalDateTime

object MainProducer {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Charger les configurations depuis fichier de configuration `src/main/resources/application.conf`
    val config = ConfigFactory.load()
    val taxiConfig = config.getConfig("taxi.producer")
    
    // Extraire les paramètres de configuration
    val batchSize = taxiConfig.getConfig("data").getInt("batchSize")
    val intervalSeconds = taxiConfig.getConfig("data").getInt("intervalSeconds")
    
    logger.info(s"Initialisation du Producer de trajets de taxi avec " +
                s"taille de batch: $batchSize, intervalle: $intervalSeconds secondes")
    
    // session Spark avec configuration pour la mémoire
    val spark = SparkSession.builder()
      .appName("YellowTaxiTripProducer")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2") // Réduire pour les petits jeux de données
      .config("spark.memory.fraction", "0.7")
      .config("spark.memory.storageFraction", "0.2")
      .getOrCreate()
    
    try {
      val dataOps = new ProducerOperations(spark, taxiConfig)
      
      // Configurer le FileSender pour écrire dans des fichiers
      val fileConfig = taxiConfig.getConfig("file") // outputDir (Répertoire où écrire les fichiers JSON) et filePrefix (Préfixe pour les noms de fichiers)
      val fileSender = new FileSender(fileConfig)
      
      val sourceFile = taxiConfig.getConfig("data").getString("sourceFile")
      //logger.info(s"Débogage du fichier CSV source: $sourceFile")
      // CsvDebugger.debugCsvFile(sourceFile)
      
      logger.info("Chargement des données...")
      
      // Charger toutes les données en une fois
      val df = dataOps.loadTripData()
      
      // Vérifier que le DataFrame contient des données
      val rowCount = df.count()
      if (rowCount == 0) {
        logger.error("ERREUR CRITIQUE: Le fichier CSV a été trouvé mais ne contient aucune donnée valide!")
                
        // Créer un fichier pour indiquer qu'il y a eu un problème
        val errorFile = new File("data/ERROR_NO_DATA_FOUND.txt")
        val writer = new PrintWriter(errorFile)
        try {
          writer.println(s"Erreur lors du chargement des données: Aucune donnée valide trouvée")
          writer.println(s"Fichier source: ${taxiConfig.getConfig("data").getString("sourceFile")}")
          writer.println(s"Date et heure: ${LocalDateTime.now()}")
          writer.println("Vérifiez que le fichier CSV est au bon format et que le schéma est correctement défini.")
        } finally {
          writer.close()
        }
        
        throw new RuntimeException("Aucune donnée valide trouvée dans le fichier CSV")
      }
      
      logger.info(s"Données chargées et triées avec succès: $rowCount enregistrements")
      
      // Traiter chaque lot avec délai - approche simplifiée
      val processingFuture = Future {
        var currentPosition = 0
        val totalRecords = df.count().toInt
        logger.info(s"Total des enregistrements à traiter: $totalRecords")
        
        logger.info("Mise en cache des données pour optimiser les performances")
        df.persist()
        
        val firstRow = df.first()
        logger.info(s"Premier enregistrement chargé: ${firstRow.toString().take(100)}...")
        
        var batchNumber = 0
        while (currentPosition < totalRecords) {
          batchNumber += 1
          logger.info(s"Traitement du batch $batchNumber, position: $currentPosition")
          
          try {
            // Extraire un batch de taille fixe
            val batchDf = dataOps.extractBatch(df, currentPosition, batchSize)
            
            // Collecter les données à envoyer (sans passer par JSON d'abord)
            val rows = batchDf.collect()
            logger.info(s"Batch récupéré avec ${rows.length} lignes")
            
            if (rows.nonEmpty) {
              // Convertir les lignes en JSON
              val jsonMessages = rows.map(_.json)
              
              // Envoyer les messages
              fileSender.writeJsonMessages(jsonMessages)
            } else {
              logger.warn(s"Batch $batchNumber est vide, aucune donnée à envoyer")
            }
          } catch {
            case e: Exception => 
              logger.error(s"Erreur lors du traitement du batch $batchNumber: ${e.getMessage}", e)
          }
          
          // position suivante
          currentPosition += batchSize
          
          // Attendre avant le traitement du prochain lot
          if (currentPosition < totalRecords) {
            logger.info(s"Attente de $intervalSeconds secondes avant le prochain batch")
            Thread.sleep(intervalSeconds * 1000)
          }
        }
      }
      
      Await.result(processingFuture, Duration.Inf)
      logger.info("Streaming de données terminé avec succès")
      
    } catch {
      case e: Exception =>
        logger.error(s"Une erreur s'est produite: ${e.getMessage}", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}