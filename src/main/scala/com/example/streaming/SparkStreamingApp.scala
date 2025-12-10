package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point
import java.util.concurrent.TimeUnit

object SparkStreamingApp {

  def main(args: Array[String]): Unit = {

    // Paramètres Kafka
    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val kafkaTopic = sys.props.getOrElse("topic", "uber_topic")

    // Paramètres InfluxDB
    val influxURL = sys.props.getOrElse("influx.url", "http://localhost:8086")
    val influxUser = sys.props.getOrElse("influx.user", "admin")
    val influxPassword = sys.props.getOrElse("influx.password", "admin123")
    val influxBucket = sys.props.getOrElse("influx.bucket", "uber_bucket")

    val spark = SparkSession.builder()
      .appName("UberStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Lire le flux Kafka
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Définir le schéma des données
    val schema = new StructType()
      .add("ride_id", StringType)
      .add("pickup_datetime", StringType)
      .add("dropoff_datetime", StringType)
      .add("vehicle_type", StringType)
      .add("passenger_count", IntegerType)
      .add("fare_amount", DoubleType)

    // Convertir JSON en DataFrame structuré
    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // Ajouter timestamp pour l’agrégation
    val dfWithTs = parsedDF.withColumn(
      "pickup_ts",
      to_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss")
    )

    // Agrégation par fenêtre et type de véhicule
    val aggDF = dfWithTs
      .withWatermark("pickup_ts", "2 minutes")
      .groupBy(
        window($"pickup_ts", "1 minute", "30 seconds"),
        $"vehicle_type"
      )
      .agg(
        count("*").alias("ride_count"),
        avg($"fare_amount").alias("avg_fare")
      )
      .select(
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"vehicle_type",
        $"ride_count",
        $"avg_fare"
      )

    val outputDir = "/tmp/uber_stream_output"

    // 1️⃣ Écriture Parquet pour sauvegarde historique
    aggDF.writeStream
      .format("parquet")
      .option("path", s"$outputDir/parquet")
      .option("checkpointLocation", s"$outputDir/checkpoint_parquet")
      .outputMode("append")
      .start()

    // 2️⃣ Écriture dans InfluxDB pour Grafana
    aggDF.writeStream
      .foreachBatch { (batchDF, batchId) =>
        val influx = InfluxDBFactory.connect(influxURL, influxUser, influxPassword)
        batchDF.collect().foreach { row =>
          val point = Point.measurement("rides")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("vehicle_type", row.getAs[String]("vehicle_type"))
            .addField("ride_count", row.getAs[Long]("ride_count"))
            .addField("avg_fare", row.getAs[Double]("avg_fare"))
            .build()
          influx.write(influxBucket, "autogen", point)
        }
      }
      .outputMode("update")
      .start()

    println("📡 Spark Streaming Consumer started. Listening to Kafka and writing to Parquet + InfluxDB.")

    spark.streams.awaitAnyTermination()
  }
}
