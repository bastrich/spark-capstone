package com.bastrich

import com.bastrich.entities.input.{Amount, Position}
import com.bastrich.entities.output.AmountResult
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object AmountJob {

  def calculate(positions: Dataset[Position], amounts: Dataset[Amount], spark: SparkSession): Dataset[AmountResult] = {
    import spark.implicits._
    positions
      .join(
        amounts
          .withColumn("rank", rank().over(Window.partitionBy("id").orderBy($"eventTime".desc)))
          .where($"rank" === 1),
        "id"
      )
      .select($"id".as("positionId"), $"warehouse", $"product", $"amount")
      .as[AmountResult]
  }
}
