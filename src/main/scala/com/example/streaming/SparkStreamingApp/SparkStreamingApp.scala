package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UberStreamingApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic = sys.props.getOrElse("topic", "uber_topic")
    val outputDir = "/tmp/uber_stream_output"

    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val schema = new StructType()
      .add("ride_id", StringType)
      .add("pickup_datetime", StringType)
      .add("dropoff_datetime", StringType)
      .add("vehicle_type", StringType)
      .add("passenger_count", IntegerType)
      .add("fare_amount", DoubleType)

    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    val dfWithTs = parsedDF.withColumn(
      "pickup_ts",
      to_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss")
    )

    val agg = dfWithTs
      .withWatermark("pickup_ts", "2 minutes")
      .groupBy(
        window($"pickup_ts", "1 minute", "30 seconds"),
        $"vehicle_type"
      )
      .agg(
        count("*").alias("ride_count"),
        avg($"fare_amount").alias("avg_fare")
      )

    val finalDF = agg.select(
      $"window.start".alias("window_start"),
      $"window.end".alias("window_end"),
      $"vehicle_type",
      $"ride_count",
      $"avg_fare"
    )

    // Write Parquet
    finalDF.writeStream
      .format("parquet")
      .option("path", s"$outputDir/parquet")
      .option("checkpointLocation", s"$outputDir/checkpoint_parquet")
      .outputMode("append")
      .start()

    // Write back to Kafka
    finalDF.select(to_json(struct($"*")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("topic", "uber_output")
      .option("checkpointLocation", s"$outputDir/checkpoint_kafka")
      .outputMode("append")
      .start()

    println("📡 Uber Streaming Consumer Started...")
    spark.streams.awaitAnyTermination()
  }
}
