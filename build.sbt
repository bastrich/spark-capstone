name := "spark-capstone"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.2"
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
