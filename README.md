# Uber Ride Streaming Analytics

This project implements a **real-time Uber ride analytics pipeline** using **Apache Spark Structured Streaming** and **Kafka**. Streaming results are stored in **InfluxDB** and visualized using **Grafana**.  

---

## 1️⃣ Project Structure

uber-streaming/
├─ build.sbt
├─ docker-compose.yml
├─ .gitignore
├─ README.md
└─ src/
└─ main/
└─ scala/
└─ com/
└─ example/
├─ producer/
│ └─ KafkaProducerApp.scala
└─ streaming/
└─ SparkStreamingApp.scala



---

## 2️⃣ Features

- Reads Uber ride bookings from a CSV file  
- Streams data into **Kafka**  
- Consumes Kafka data using **Spark Structured Streaming**  
- Aggregates rides by vehicle type and time window  
- Stores streaming results in **InfluxDB**  
- Visualizes data in **Grafana dashboards**  

---

## 3️⃣ Prerequisites

- **Docker & Docker Compose**  
- **Java 17**  
- **SBT**  
- CSV file: `ncr_ride_bookings.csv`  

---

## Start Project

Start the required services:

```bash
docker-compose up -d

create Topics
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 8 --topic uber_topic
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 8 --topic uber_output

to create topics or reset them:

 run ./reset.sh 

sbt clean compile
sbt clean assembly


sbt clean compile
sbt "runMain com.example.producer.KafkaProducerApp"

or

spark-submit \
  --master local[8] \
  --driver-memory 2G \
  --class com.example.producer.KafkaProducerApp \
  target/scala-2.13/uber-streaming-assembly-0.1.0.jar \
  --kafka.bootstrap localhost:9092 \
  --topic uber_topic \
  --file /absolute/path/to/ncr_ride_bookings.csv \
  --partitions 8 \
  --delay.ms 0


spark-submit \
  --master local[8] \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=8 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  --class com.example.producer.KafkaProducerApp \
  target/scala-2.13/uber-streaming-assembly-0.1.0.jar \
  --kafka.bootstrap localhost:9092 \
  --topic uber_topic \
  --file /path/to/ncr_ride_bookings.csv \
  --partitions 8 \
  --delay.ms 0




sbt "runMain com.example.streaming.SparkStreamingApp"

or

spark-submit \
  --master local[8] \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=8 \
  --class com.example.streaming.SparkStreamingApp \
  target/scala-2.13/uber-streaming-assembly-0.1.0.jar \
  --kafka.bootstrap localhost:9092 \
  --kafka.topic uber_topic \
  --influx.url http://localhost:8086 \
  --influx.user admin \
  --influx.password admin123 \
  --influx.bucket uber_bucket \
  --parquet.out /tmp/uber_stream_output/parquet


spark-submit \
  --master local[8] \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=8 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  --class com.example.streaming.SparkStreamingApp \
  target/scala-2.13/uber-streaming-assembly-0.1.0.jar \
  ...

