import java.sql.Timestamp

import com.bastrich.AmountJob
import com.bastrich.entities.input.{Amount, Position}
import com.bastrich.entities.output.AmountResult
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class AmountJobTest extends BaseSpec {

  import spark.implicits._

  val positions =
    List(
      Position(1, "w1", "p1", new Timestamp(111)),
      Position(2, "w1", "p2", new Timestamp(222)),
      Position(3, "w3", "p1", new Timestamp(333)),
      Position(777, "w1", "p1", new Timestamp(333)),
      Position(888, "w777", "p111", new Timestamp(444))
    ).toDS

  val amounts =
    List(
      Amount(1, BigDecimal(40), new Timestamp(123)),
      Amount(2, BigDecimal(50), new Timestamp(456)),
      Amount(777, BigDecimal(60), new Timestamp(777)),
      Amount(888, BigDecimal(70), new Timestamp(888)),
      Amount(111, BigDecimal(80), new Timestamp(999)),
      Amount(1, BigDecimal(30), new Timestamp(456)),
    ).toDS

  it("test calculating amounts") {
    AmountJob.calculate(positions, amounts, spark).collectAsList().asScala shouldEqual List(
      AmountResult(1, "w1", "p1", BigDecimal(30)),
      AmountResult(2, "w1", "p2", BigDecimal(50)),
      AmountResult(777, "w1", "p1", BigDecimal(60)),
      AmountResult(888, "w777", "p111", BigDecimal(70))
    )
  }
}
