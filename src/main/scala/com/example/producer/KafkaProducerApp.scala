package com.example.producer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.Properties
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
import java.sql.{Date, Timestamp}

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaCSVProducerApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic          = sys.props.getOrElse("topic", "uber_topic")
    val csvPath        = sys.props.getOrElse("file", "ncr_ride_bookings.csv")
    val partitions     = sys.props.getOrElse("partitions", "8").toInt
    val sendDelayMs    = sys.props.getOrElse("delay.ms", "0").toInt

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .repartition(partitions)

    println(s"Loaded ${df.count()} rows, repartitioned to $partitions partitions")

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("acks", "1")
    props.put("linger.ms", "20")
    props.put("batch.size", (64*1024).toString)
    props.put("compression.type", "lz4")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    df.foreachPartition { partition: Iterator[Row] =>
      if (partition.nonEmpty) {
        val producer = new KafkaProducer[String, String](props)
        try {
          val futures = partition.map { row =>
            // Combine Date + Time for pickup_datetime safely
            val pickupDate = row.getAs[Any]("Date") match {
              case d: Date => d.toString
              case ts: Timestamp => ts.toString
              case s: String => s
              case null => ""
            }
            val pickupTime = row.getAs[Any]("Time") match {
              case t: String => t
              case ts: Timestamp => ts.toString.split(" ")(1) // get time part
              case null => ""
            }

            val pickupDateTime =
              if (pickupDate.nonEmpty && pickupTime.nonEmpty) s"$pickupDate $pickupTime"
              else pickupDate // fallback if Time is missing

            // Safely parse numeric Booking Value
            val bookingValue = row.getAs[Any]("Booking Value") match {
              case d: Double => d
              case i: Int => i.toDouble
              case s: String => Try(s.toDouble).getOrElse(0.0)
              case _ => 0.0
            }

            // Build JSON safely
            val map = row.getValuesMap[Any](row.schema.fieldNames)
            val cleanMap = map.map { case (k,v) =>
              k -> (v match {
                case null => ""
                case s: String => s.replace("\"","")
                case d: Date => d.toString
                case ts: Timestamp => ts.toString
                case other => other.toString
              })
            } + ("pickup_datetime" -> pickupDateTime) + ("booking_value" -> bookingValue.toString)

            val json = cleanMap.map { case (k,v) => "\"" + k + "\":\"" + v + "\"" }.mkString("{",",","}")

            val key = Option(row.getAs[Any]("Vehicle Type")).map(_.toString).orNull
            val rec = new ProducerRecord[String, String](topic, key, json)

            Future {
              val meta: RecordMetadata = producer.send(rec).get()
              meta
            }
          }.toList

          Await.result(Future.sequence(futures), Duration.Inf)
        } catch {
          case ex: Exception => println(s"[Producer] exception in partition send: ${ex.getMessage}")
        } finally {
          producer.flush()
          producer.close()
        }
      }
      if (sendDelayMs > 0) Thread.sleep(sendDelayMs)
    }

    println(s"Finished sending to Kafka topic: $topic")
    spark.stop()
  }
}
