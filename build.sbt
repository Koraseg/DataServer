name := "DataServer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % "2.5.6",
  "joda-time" % "joda-time" % "2.8.1",
  "org.json4s" % "json4s-native_2.11" % "3.5.3"
)