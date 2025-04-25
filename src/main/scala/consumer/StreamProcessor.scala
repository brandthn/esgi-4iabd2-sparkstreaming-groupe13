package consumer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * StreamProcessor effectue les transformations sur les données de streaming
 * et écrit les résultats pour la visualisation
 */
class StreamProcessor(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Configuration de sortie
  private val outputConfig = config.getConfig("output")
  private val outputEnabled = outputConfig.getBoolean("enabled")
  private val outputDir = outputConfig.getString("directory")
  private val outputFormat = outputConfig.getString("format")
  
  /**
   * Ajoute un timestamp de traitement pour suivre quand un batch a été traité
   */
  private def addProcessingTimeColumns(df: DataFrame): DataFrame = {
    val now = new Timestamp(System.currentTimeMillis())
    val batchId = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    
    df.withColumn("processing_time", lit(now))
      .withColumn("batch_id", lit(batchId))
  }
  
  /**
   * Effectue les agrégations par lieu de prise en charge (PULocationID)
   */
  def aggregateByPickupLocation(streamDF: DataFrame): DataFrame = {
    logger.info("Performing aggregation by pickup location")
    
    val aggregatedByPickup = streamDF
      .groupBy("PULocationID", "batch_id")
      .agg(
        count("*").as("trip_count"),
        sum("fare_amount").as("total_fare"),
        avg("trip_distance").as("avg_distance"),
        sum("passenger_count").as("total_passengers")
      )
      .withColumn("aggregation_type", lit("pickup_location"))
    
    aggregatedByPickup
  }
  
  /**
   * Effectue les agrégations par lieu de dépose (DOLocationID)
   */
  def aggregateByDropoffLocation(streamDF: DataFrame): DataFrame = {
    logger.info("Performing aggregation by dropoff location")
    
    val aggregatedByDropoff = streamDF
      .groupBy("DOLocationID", "batch_id")
      .agg(
        count("*").as("trip_count"),
        sum("fare_amount").as("total_fare"),
        avg("trip_distance").as("avg_distance"),
        sum("passenger_count").as("total_passengers")
      )
      .withColumn("aggregation_type", lit("dropoff_location"))
    
    aggregatedByDropoff
  }
  
  /**
   * Combine les agrégations en un seul DataFrame
   */
  def combineAggregations(pickupAgg: DataFrame, dropoffAgg: DataFrame): DataFrame = {
    pickupAgg
      .withColumnRenamed("PULocationID", "location_id")
      .unionByName(
        dropoffAgg.withColumnRenamed("DOLocationID", "location_id"),
        allowMissingColumns = true
      )
  }
  
  /**
   * Initie le traitement du streaming avec les transformations souhaitées
   */
  def processStream(inputStream: DataFrame): StreamingQuery = {
    logger.info("Starting stream processing")
    
    // Ajouter le timestamp et l'ID de batch
    val streamWithMetadata = addProcessingTimeColumns(inputStream)
    
    // Créer une vue temporaire pour pouvoir référencer le stream dans SQL
    streamWithMetadata.createOrReplaceTempView("taxi_trips")
    
    // Query de base: juste visualiser les données brutes
    val rawDataQuery = """
      SELECT 
        batch_id, 
        VendorID, 
        tpep_pickup_datetime, 
        tpep_dropoff_datetime,
        PULocationID,
        DOLocationID,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount,
        payment_type
      FROM taxi_trips
    """
    
    val rawData = spark.sql(rawDataQuery)
    
    // Créer répertoire de sortie s'il n'existe pas
    if (outputEnabled) {
      logger.info(s"Output is enabled, writing to $outputDir")
      if (!Files.exists(Paths.get(outputDir))) {
        Files.createDirectories(Paths.get(outputDir))
      }
      
      // Écrire les agrégations
      val query = rawData.writeStream
        .outputMode(OutputMode.Append())
        .format(outputFormat)
        .option("path", s"$outputDir/raw")
        .option("checkpointLocation", s"$outputDir/checkpoints/raw")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          logger.info(s"Processing batch $batchId")
          
          // Écrire les données brutes
          batchDF
            .write
            .format(outputFormat)
            .mode("append")
            .save(s"$outputDir/raw")
          
          // Aggréger par lieu de prise en charge
          val pickupAgg = aggregateByPickupLocation(batchDF)
          pickupAgg
            .write
            .format(outputFormat)
            .mode("append")
            .save(s"$outputDir/pickup_agg")
          
          // Aggréger par lieu de dépose
          val dropoffAgg = aggregateByDropoffLocation(batchDF)
          dropoffAgg
            .write
            .format(outputFormat)
            .mode("append")
            .save(s"$outputDir/dropoff_agg")
          
          // Combinaison pour visualisation facile
          val combinedAgg = combineAggregations(pickupAgg, dropoffAgg)
          combinedAgg
            .write
            .format(outputFormat)
            .mode("append")
            .save(s"$outputDir/combined_agg")
            
          // Afficher un échantillon pour le débogage
          logger.info(s"Sample of raw data in batch $batchId:")
          batchDF.show(5, truncate = false)
          
          logger.info(s"Sample of aggregated data in batch $batchId:")
          combinedAgg.show(5, truncate = false)
        }
        .start()
        
      logger.info("Stream processing started")
      query
    } else {
      logger.info("Output is disabled, not writing to disk")
      rawData.writeStream
        .format("console")
        .outputMode("append")
        .start()
    }
  }
}