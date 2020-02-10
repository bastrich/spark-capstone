import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

trait BaseSpec extends FunSpec{
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .getOrCreate()
  }
}
