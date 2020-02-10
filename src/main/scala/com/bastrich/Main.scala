package com.bastrich

import com.bastrich.entities.input.{Amount, Position}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App {
  val config = ConfigFactory.load()
  val spark = SparkSession
    .builder()
    .config(new SparkConf())
    .appName("warehouse")
    .getOrCreate()

  try {
    import spark.implicits._

    val positions = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(config.getString("capstone.input.positions"))
      .as[Position]

    val amounts = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(config.getString("capstone.input.amounts"))
      .as[Amount]

    val amountResult = AmountJob.calculate(positions, amounts, spark).cache()
    val statisticsResult = StatisticsJob.calculate(amountResult, spark)

    amountResult.write.csv(config.getString("capstone.output.amounts"))
    statisticsResult.write.csv(config.getString("capstone.output.statistics"))
  } finally {
    spark.stop()
  }
}
