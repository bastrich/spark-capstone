import java.sql.Timestamp

import com.bastrich.{AmountJob, StatisticsJob}
import com.bastrich.entities.input.{Amount, Position}
import com.bastrich.entities.output.{AmountResult, StatisticsResult}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import Matchers._
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

class Test extends FunSpec {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .getOrCreate()
  }

  import spark.implicits._

  val positions =
    List(
      Position(1, "w1", "p1", new Timestamp(111)),
      Position(2, "w1", "p2", new Timestamp(222)),
      Position(3, "w3", "p1", new Timestamp(333)),
      Position(777, "w1", "p1", new Timestamp(333)),
      Position(888, "w777", "p111", new Timestamp(444))
    ).toDS()

  val amounts =
    List(
      Amount(1, BigDecimal(40), new Timestamp(123)),
      Amount(2, BigDecimal(50), new Timestamp(456)),
      Amount(777, BigDecimal(60), new Timestamp(777)),
      Amount(888, BigDecimal(70), new Timestamp(888)),
      Amount(111, BigDecimal(80), new Timestamp(999)),
      Amount(1, BigDecimal(30), new Timestamp(456)),
    ).toDS()

  it("test") {
    val amountsResult = AmountJob.calculate(positions, amounts, spark).cache()

    amountsResult.collectAsList().asScala shouldEqual List(
      AmountResult(1, "w1", "p1", BigDecimal(30)),
      AmountResult(2, "w1", "p2", BigDecimal(50)),
      AmountResult(777, "w1", "p1", BigDecimal(60)),
      AmountResult(888, "w777", "p111", BigDecimal(70))
    )

    val statisticsResult = StatisticsJob.calculate(amountsResult, spark)

    statisticsResult.collectAsList().asScala shouldEqual List(
      StatisticsResult("w1", "p1", BigDecimal(60), BigDecimal(30), BigDecimal(45)),
      StatisticsResult("w1", "p2", BigDecimal(50), BigDecimal(50), BigDecimal(50)),
      StatisticsResult("w777", "p111", BigDecimal(70), BigDecimal(70), BigDecimal(70))
    )
  }
}
