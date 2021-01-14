package DFbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data

object ReadCSV {
  def main(args: Array[String]): Unit = {
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark DF")
      .getOrCreate()

    val properties = Map("header " -> "true", "inferSchema" -> "true")
    val df = spark.read
      .option("head", "true")
      .option("inferSchema", "true")
      .options(properties)
      .csv("src/main/resources/people.csv")

    df.printSchema()

    df.show()
  }
  def topTwoEmp(df:DataFrame): Unit = {

    df.groupBy("_c7").count().sort("_c7").show(2)

    df.show()
  }
  }
}
