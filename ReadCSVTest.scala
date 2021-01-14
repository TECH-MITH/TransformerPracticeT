package DFbasics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class ReadCSVTest extends AnyFlatSpec {

  val spark = SparkSession.builder()
    .appName("HelloSpark")
    .master("local")
    .getOrCreate()

  behavior of "Read CSV"

  it should "replace null value with unknown" in {
    val df : DataFrame = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("rc/main/resources/people.csv")
    df.show()
  }

}
