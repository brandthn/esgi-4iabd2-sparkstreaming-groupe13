package producer

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Responsable de l'écriture des données dans un fichier pour simulation de streaming
 */
class FileSender(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Configuration pour le répertoire de sortie et le préfixe de fichier
  private val outputDir = config.getString("outputDir")
  private val filePrefix = config.getString("filePrefix")
  
  // Configuration pour debug
  private val debugEnabled = if (config.hasPath("debugOutput.enabled")) 
    config.getBoolean("debugOutput.enabled") else false
  private val debugOutputFile = if (debugEnabled && config.hasPath("debugOutput.file")) 
    Some(config.getString("debugOutput.file")) else None
  private val debugOutputConsole = if (debugEnabled && config.hasPath("debugOutput.console")) 
    config.getBoolean("debugOutput.console") else false
  
  /**
   * Enregistre le contenu du batch pour déboguer
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
  
  // Compteur de batches
  private var batchCounter = 0
  
  /**
   * Écrit un lot de messages JSON dans un fichier
   */
  def writeJsonMessages(jsonMessages: Array[String]): Unit = {
    batchCounter += 1
    logger.info(s"Écriture du batch #$batchCounter avec ${jsonMessages.length} messages")
    
    // Debug le batch
    debugBatch(batchCounter, jsonMessages)
    
    if (jsonMessages.isEmpty) {
      logger.warn("Batch vide, aucune écriture nécessaire")
      
      // Pour faciliter le débogage, créons un fichier vide avec un message
      val outputDirectory = new File(outputDir)
      if (!outputDirectory.exists()) {
        outputDirectory.mkdirs()
      }
      
      // Créer un nom de fichier avec horodatage pour le fichier vide
      val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
      val emptyFileName = s"$outputDir/empty-batch$batchCounter-$timestamp.txt"
      
      try {
        val writer = new PrintWriter(new File(emptyFileName))
        try {
          writer.println(s"Batch #$batchCounter était vide - Aucune donnée à écrire")
          logger.info(s"Fichier de marqueur pour batch vide créé: $emptyFileName")
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception => 
          logger.error(s"Erreur lors de l'écriture du fichier de marqueur: ${e.getMessage}", e)
      }
      
      return
    }
    
    // Créer le répertoire de sortie s'il n'existe pas
    val outputDirectory = new File(outputDir)
    if (!outputDirectory.exists()) {
      outputDirectory.mkdirs()
    }
    
    // Créer un nom de fichier avec horodatage
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val outputFileName = s"$outputDir/$filePrefix-batch$batchCounter-$timestamp.json"
    
    try {
      // Vérification supplémentaire des données avant écriture
      logger.info(s"Préparation à l'écriture de ${jsonMessages.length} messages")
      if (jsonMessages.length > 0) {
        logger.info(s"Premier message: ${jsonMessages(0).take(100)}...")
      }
      
      val writer = new PrintWriter(new File(outputFileName))
      try {
        // Écrire chaque message JSON sur une ligne
        jsonMessages.foreach(writer.println)
        logger.info(s"Batch #$batchCounter écrit avec succès dans $outputFileName")
      } finally {
        writer.close()
      }
    } catch {
      case e: Exception => 
        logger.error(s"Erreur lors de l'écriture du batch dans le fichier: ${e.getMessage}", e)
    }
  }
}