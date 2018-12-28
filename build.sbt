name := "CarsInfo"

version := "0.2"

scalaVersion := "2.11.8"

logLevel := Level.Warn

Compile/mainClass := Some("com.carsstats.spark.core.CarsStats")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.5"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"