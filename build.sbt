name         := "Log Analysis"
version      := "1.0"
organization := "com.gantek"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0" ,
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.vegas-viz" %% "vegas-spark" % "0.3.11",
  "joda-time" % "joda-time" % "2.9.9"
)

resolvers += Resolver.mavenLocal
