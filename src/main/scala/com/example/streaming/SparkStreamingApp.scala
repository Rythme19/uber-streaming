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
      .appName("UberStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val kafkaTopic = sys.props.getOrElse("kafka.topic", "uber_topic")

    val influxURL = sys.props.getOrElse("influx.url", "http://localhost:8086")
    val influxUser = sys.props.getOrElse("influx.user", "admin")
    val influxPassword = sys.props.getOrElse("influx.password", "admin123")
    val influxDB = sys.props.getOrElse("influx.bucket", "uber_bucket")

    // ============================
    // Schema (Kafka JSON)
    // ============================
    val schema = new StructType()
      .add("Booking ID", StringType)
      .add("Customer ID", StringType)
      .add("Vehicle Type", StringType)
      .add("pickup_datetime", StringType)
      .add("booking_value", DoubleType)

    // ============================
    // Read Kafka
    // ============================
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    val parsed = kafkaDF
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // ============================
    // Safe timestamp parsing
    // ============================
    val clean = parsed
      .withColumn("pickup_ts",
        expr("try_to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss')")
      )
      .filter($"pickup_ts".isNotNull && $"booking_value".isNotNull)

    // ============================
    // Time window
    // ============================
    val withWindow = clean
      .withWatermark("pickup_ts", "2 minutes")

    // ============================
    // METRIC 1 — rides per vehicle per minute
    // ============================
    val ridesPerVehicle = withWindow
      .groupBy(window($"pickup_ts", "1 minute"), $"Vehicle Type")
      .agg(count("*").alias("ride_count"))

    // ============================
    // METRIC 2 — revenue per vehicle
    // ============================
    val revenuePerVehicle = withWindow
      .groupBy(window($"pickup_ts", "1 minute"), $"Vehicle Type")
      .agg(sum($"booking_value").alias("total_revenue"))

    // ============================
    // METRIC 3 — average fare per vehicle
    // ============================
    val avgFare = withWindow
      .groupBy(window($"pickup_ts", "1 minute"), $"Vehicle Type")
      .agg(avg($"booking_value").alias("avg_fare"))

    // ============================
    // METRIC 4 — total rides per minute
    // ============================
    val totalRides = withWindow
      .groupBy(window($"pickup_ts", "1 minute"))
      .agg(count("*").alias("total_rides"))

    // ============================
    // Write all metrics to InfluxDB
    // ============================
    writeToInflux("rides_per_vehicle", ridesPerVehicle, influxURL, influxUser, influxPassword, influxDB)
    writeToInflux("revenue_per_vehicle", revenuePerVehicle, influxURL, influxUser, influxPassword, influxDB)
    writeToInflux("avg_fare_per_vehicle", avgFare, influxURL, influxUser, influxPassword, influxDB)
    writeToInflux("total_rides", totalRides, influxURL, influxUser, influxPassword, influxDB)

    println(" Uber Streaming Consumer running...")
    spark.streams.awaitAnyTermination()
  }

  // ======================================================
  // Generic InfluxDB Writer (works for all aggregations)
  // ======================================================
  def writeToInflux(
                     measurement: String,
                     df: org.apache.spark.sql.DataFrame,
                     influxURL: String,
                     influxUser: String,
                     influxPassword: String,
                     influxDB: String
                   ) = {

    df.writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

      batchDF.foreachPartition { (partition: Iterator[org.apache.spark.sql.Row]) =>

        val influx = InfluxDBFactory.connect(influxURL, influxUser, influxPassword)
        influx.setDatabase(influxDB)

        partition.foreach { row =>
          val point = Point.measurement(measurement)
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

          row.schema.fields.foreach { field =>
            if (!field.name.contains("window")) {
              val v = row.getAs[Any](field.name)
              if (v != null) {
                point.addField(field.name.replace(" ", "_"), v.toString)
              }
            }
          }

          influx.write(point.build())
        }

        influx.close()
      }
    }.start()
  }
}
