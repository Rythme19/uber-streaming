package com.example.producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaCSVProducerApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic = sys.props.getOrElse("topic", "uber_topic")
    val csvPath = sys.props.getOrElse("file", "/absolute/path/to/ncr_ride_bookings.csv")

    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    val jsonDF = csvDF.select(to_json(struct(csvDF.columns.map(col): _*)).alias("value"))

    jsonDF.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("topic", topic)
      .save()

    println(s"✅ CSV data published to Kafka topic: $topic")
  }
}
