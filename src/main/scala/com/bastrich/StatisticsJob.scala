package com.bastrich

import com.bastrich.entities.output.{AmountResult, StatisticsResult}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Dataset, SparkSession}

object StatisticsJob {

  def calculate(amounts: Dataset[AmountResult], spark: SparkSession): Dataset[StatisticsResult] = {
    import spark.implicits._
    amounts
      .groupBy("warehouse", "product")
      .agg(
        max("amount").as("max"),
        min("amount").as("min"),
        avg($"amount".cast(DecimalType(34,14))).as("avg")
      )
      .as[StatisticsResult]
  }
}
