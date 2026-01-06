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
    val influxBucket = sys.props.getOrElse("influx.bucket", "uber_bucket")

    // Schema matches cleaned JSON from Producer
    val schema = new StructType()
      .add("Booking ID", StringType)
      .add("Customer ID", StringType)
      .add("Vehicle Type", StringType)
      .add("pickup_datetime", StringType)
      .add("booking_value", DoubleType)

    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // Use try_to_timestamp to tolerate invalid strings
    val dfWithTs = parsedDF.withColumn(
      "pickup_ts",
      expr("try_to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss')")
    ).filter($"pickup_ts".isNotNull)

    val aggDF = dfWithTs
      .withWatermark("pickup_ts", "2 minutes")
      .groupBy(
        window($"pickup_ts", "1 minute", "30 seconds"),
        $"Vehicle Type"
      )
      .agg(
        count("*").alias("ride_count"),
        avg($"booking_value").alias("avg_fare")
      )
      .select(
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"Vehicle Type",
        $"ride_count",
        $"avg_fare"
      )

    val outputDir = "/tmp/uber_stream_output"

    // Write to Parquet
    aggDF.writeStream
      .format("parquet")
      .option("path", s"$outputDir/parquet")
      .option("checkpointLocation", s"$outputDir/checkpoint_parquet")
      .outputMode("append")
      .start()

    // Write to InfluxDB
    aggDF.writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
      batchDF.foreachPartition { (partition: Iterator[org.apache.spark.sql.Row]) =>
        val influx = InfluxDBFactory.connect(influxURL, influxUser, influxPassword)
        partition.foreach { row =>
          val point = Point.measurement("rides")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("vehicle_type", row.getAs[String]("Vehicle Type"))
            .addField("ride_count", row.getAs[Long]("ride_count"))
            .addField("avg_fare", row.getAs[Double]("avg_fare"))
            .build()
          influx.write(point)
        }
        influx.close()
      }
    }.start()

    println("📡 Uber Streaming Consumer Started...")
    spark.streams.awaitAnyTermination()
  }
}
