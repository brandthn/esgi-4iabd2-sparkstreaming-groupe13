// src/main/scala/com/taxi/producer/TaxiRecord.scala
package com.taxi.producer

import java.sql.Timestamp
import org.apache.spark.sql.Row

case class TaxiRecord(
  vendorID: Int,
  tpepPickupDatetime: Timestamp,
  tpepDropoffDatetime: Timestamp,
  passengerCount: Int,
  tripDistance: Double,
  puLocationID: Int,
  doLocationID: Int,
  fareAmount: Double,
  tipAmount: Double,
  totalAmount: Double
)

object TaxiRecord {
  // Convertir une Row Spark en TaxiRecord
  def fromRow(row: Row): TaxiRecord = {
    TaxiRecord(
      vendorID = row.getAs[Int]("VendorID"),
      tpepPickupDatetime = row.getAs[Timestamp]("tpep_pickup_datetime"),
      tpepDropoffDatetime = row.getAs[Timestamp]("tpep_dropoff_datetime"),
      passengerCount = row.getAs[Int]("passenger_count"),
      tripDistance = row.getAs[Double]("trip_distance"),
      puLocationID = row.getAs[Int]("PULocationID"),
      doLocationID = row.getAs[Int]("DOLocationID"),
      fareAmount = row.getAs[Double]("fare_amount"),
      tipAmount = row.getAs[Double]("tip_amount"),
      totalAmount = row.getAs[Double]("total_amount")
    )
  }
  
  // Convertir un TaxiRecord en format JSON
  def toJson(record: TaxiRecord): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    
    implicit val formats = Serialization.formats(NoTypeHints)
    
    write(record)
  }
}