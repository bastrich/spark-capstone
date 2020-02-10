import com.bastrich.StatisticsJob
import com.bastrich.entities.output.{AmountResult, StatisticsResult}
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class StatisticsJobTest extends BaseSpec {

  import spark.implicits._

  val amounts = List(
    AmountResult(1, "w1", "p1", BigDecimal(30)),
    AmountResult(2, "w1", "p2", BigDecimal(50)),
    AmountResult(777, "w1", "p1", BigDecimal(60)),
    AmountResult(888, "w777", "p111", BigDecimal(70))
  ).toDS

  it("test calculating statistics") {
    StatisticsJob.calculate(amounts, spark).collectAsList().asScala shouldEqual List(
      StatisticsResult("w1", "p1", BigDecimal(60), BigDecimal(30), BigDecimal(45)),
      StatisticsResult("w1", "p2", BigDecimal(50), BigDecimal(50), BigDecimal(50)),
      StatisticsResult("w777", "p111", BigDecimal(70), BigDecimal(70), BigDecimal(70))
    )
  }
}
