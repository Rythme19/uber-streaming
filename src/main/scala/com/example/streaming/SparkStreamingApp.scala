package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point
import java.util.concurrent.TimeUnit

object SparkStreamingApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UberStreamingApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Kafka settings
    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val kafkaTopic     = sys.props.getOrElse("kafka.topic", "uber_topic")

    // InfluxDB settings
    val influxURL      = sys.props.getOrElse("influx.url", "http://localhost:8086")
    val influxUser     = sys.props.getOrElse("influx.user", "admin")
    val influxPassword = sys.props.getOrElse("influx.password", "admin123")
    val influxDatabase = sys.props.getOrElse("influx.bucket", "uber_bucket")

    // Kafka input stream
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Input JSON schema
    val schema = new StructType()
      .add("ride_id", StringType)
      .add("pickup_datetime", StringType)
      .add("dropoff_datetime", StringType)
      .add("vehicle_type", StringType)
      .add("passenger_count", IntegerType)
      .add("fare_amount", DoubleType)

    // JSON parsing
    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // Convert pickup time
    val dfWithTs = parsedDF.withColumn(
      "pickup_ts",
      to_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss")
    )

    // Aggregation
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
      .select(
        $"window.start".alias("window_start"),
        $"vehicle_type",
        $"ride_count",
        $"avg_fare"
      )

    // Write also in parquet for debug
    val outputDir = "/tmp/uber_stream_output"
    agg.writeStream
      .format("parquet")
      .option("path", s"$outputDir/parquet")
      .option("checkpointLocation", s"$outputDir/checkpoint_parquet")
      .outputMode("append")
      .start()

    // ==============================
    // 🔥 Write to InfluxDB per batch
    // ==============================
    agg.writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
      batchDF.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>

        val influx = InfluxDBFactory.connect(influxURL, influxUser, influxPassword)
        influx.setDatabase(influxDatabase)
        influx.enableBatch(2000, 1000, TimeUnit.MILLISECONDS)

        rows.foreach { row =>

          val windowStart = row.getAs[java.sql.Timestamp]("window_start")

          val point = Point.measurement("rides")
            .time(windowStart.getTime, TimeUnit.MILLISECONDS)
            .tag("vehicle_type", row.getAs[String]("vehicle_type"))
            .addField("ride_count", row.getAs[Long]("ride_count"))
            .addField("avg_fare", row.getAs[Double]("avg_fare"))
            .build()

          influx.write(point)
        }

        influx.flush()
        influx.close()
      }
    }.start()

    println("📡 Uber Streaming Consumer Started…")
    spark.streams.awaitAnyTermination()
  }
}
