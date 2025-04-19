// src/main/scala/com/taxi/producer/Config.scala
package com.taxi.producer

import com.typesafe.config.ConfigFactory

object Config {
  private val config = ConfigFactory.load()
  
  object Kafka {
    private val kafkaConfig = config.getConfig("kafka")
    val bootstrapServers: String = kafkaConfig.getString("bootstrap.servers")
    val topic: String = kafkaConfig.getString("topic")
  }
  
  object Taxi {
    private val taxiConfig = config.getConfig("taxi")
    val dataPath: String = taxiConfig.getString("data.path")
    
    object Simulation {
      private val simConfig = taxiConfig.getConfig("simulation")
      val speedFactor: Int = simConfig.getInt("speed.factor")
      val batchSize: Int = simConfig.getInt("batch.size")
    }
  }
}