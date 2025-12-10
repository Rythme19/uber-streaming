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

## 4️⃣ Docker Setup

Start the required services:

```bash
docker-compose up -d

