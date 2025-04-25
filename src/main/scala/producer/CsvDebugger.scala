package producer

import java.io.File
import scala.io.Source
import org.slf4j.LoggerFactory

/**
 * Utilitaire pour déboguer les problèmes liés aux fichiers CSV
 */
object CsvDebugger {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Analyse le fichier CSV pour vérifier sa structure et son contenu
   */
  def debugCsvFile(filePath: String): Unit = {
    logger.info(s"Début de l'analyse du fichier CSV: $filePath")
    
    val file = new File(filePath)
    if (!file.exists()) {
      logger.error(s"FICHIER NON TROUVÉ: $filePath")
      logger.error(s"Chemin absolu attendu: ${file.getAbsolutePath}")
      return
    }
    
    logger.info(s"Fichier trouvé: ${file.getAbsolutePath}")
    logger.info(s"Taille du fichier: ${file.length()} octets")
    logger.info(s"Dernière modification: ${new java.util.Date(file.lastModified())}")
    
    try {
      val bufferedSource = Source.fromFile(file)
      
      // Compter les lignes
      val lineCount = bufferedSource.getLines().size
      bufferedSource.close()
      
      logger.info(s"Nombre de lignes dans le fichier: $lineCount")
      
      // Relire pour analyser les premières lignes
      val newSource = Source.fromFile(file)
      val lines = newSource.getLines().take(6).toList
      newSource.close()
      
      if (lines.isEmpty) {
        logger.error("Le fichier est vide!")
        return
      }
      
      // Analyser l'en-tête
      val header = lines.head
      logger.info(s"En-tête du CSV: $header")
      val columns = header.split(",")
      logger.info(s"Nombre de colonnes détecté: ${columns.length}")
      logger.info("Liste des colonnes:")
      columns.zipWithIndex.foreach { case (col, idx) => 
        logger.info(s"  $idx: '$col'") 
      }
      
      // Analyser les premières lignes de données
      if (lines.size > 1) {
        logger.info("Exemples de lignes de données:")
        lines.tail.zipWithIndex.foreach { case (line, idx) =>
          logger.info(s"Ligne ${idx + 1}: $line")
          
          // Analyser les champs de cette ligne
          val fields = line.split(",")
          logger.info(s"  Nombre de champs: ${fields.length} (Attendu: ${columns.length})")
          
          if (fields.length != columns.length) {
            logger.warn("  ⚠️ Le nombre de champs ne correspond pas au nombre de colonnes")
          }
          
          // Afficher quelques exemples de valeurs
          fields.take(Math.min(5, fields.length)).zipWithIndex.foreach { case (field, fidx) =>
            val columnName = if (fidx < columns.length) columns(fidx) else s"Colonne $fidx"
            logger.info(s"  Champ '$columnName': '$field'")
          }
        }
      } else {
        logger.warn("Aucune ligne de données trouvée après l'en-tête")
      }
      
    } catch {
      case e: Exception => 
        logger.error(s"Erreur lors de l'analyse du fichier CSV: ${e.getMessage}", e)
    }
    
    logger.info("Fin de l'analyse du fichier CSV")
  }
}