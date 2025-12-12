package com.example.producer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaCSVProducerApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic = sys.props.getOrElse("topic", "uber_topic")
    val csvPath = sys.props.getOrElse("file", "ncr_ride_bookings.csv")

    // Lire le CSV
    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Configuration Kafka Producer
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Envoyer par partition (batch)
    csvDF.foreachPartition { partition: Iterator[Row] =>
      val producer = new KafkaProducer[String, String](props)
      partition.foreach { row: Row =>
        // Convertir la ligne en JSON
        val json = row.getValuesMap[Any](row.schema.fieldNames).map {
          case (k, v) => s""""$k": "${v.toString}""""
        }.mkString("{", ",", "}")
        producer.send(new ProducerRecord[String, String](topic, json))
      }
      producer.flush()
      producer.close()
    }

    println(s"CSV data successfully published to Kafka topic: $topic")
    spark.stop()
  }
}
