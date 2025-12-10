name := "uber-streaming"
version := "0.1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.1" % "provided",
  "org.apache.kafka" % "kafka-clients" % "3.8.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11"
)
