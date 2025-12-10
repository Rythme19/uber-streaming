package com.example.producer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.util.Try

object KafkaProducerApp {

  def main(args: Array[String]): Unit = {

    // Lecture des arguments ou valeurs par défaut
    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic = sys.props.getOrElse("topic", "uber_topic")
    val csvFile = sys.props.getOrElse("file", "ncr_ride_bookings.csv")
    val numPartitions = sys.props.getOrElse("partitions", "5").toInt

    val spark = SparkSession.builder()
      .appName("KafkaCSVProducerApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Lire le CSV
    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFile)
      .repartition(numPartitions) // Partitionnement pour envoi par lot

    println(s" CSV loaded with ${csvDF.count()} rows, repartitioned into $numPartitions partitions.")

    // Convertir chaque partition en JSON et envoyer à Kafka
    csvDF.foreachPartition { partition =>
      val kafkaProps = new java.util.Properties()
      kafkaProps.put("bootstrap.servers", kafkaBootstrap)
      kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](kafkaProps)

      partition.foreach { row =>
        val jsonValue = csvDF.columns.map(c => s""""$c":"${row.getAs[Any](c)}"""").mkString("{", ",", "}")
        val record = new org.apache.kafka.clients.producer.ProducerRecord[String, String](topic, jsonValue)
        producer.send(record)
      }
      producer.close()
    }

    println(s"All partitions sent to Kafka topic: $topic")

    spark.stop()
  }
}
