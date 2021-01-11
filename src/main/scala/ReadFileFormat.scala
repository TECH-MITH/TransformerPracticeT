import org.apache.spark.sql.SparkSession

object ReadFileFormat {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DF file formats")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("src/main/resources/EmployeeData.json")
    jsonDF.show()
    println("Count: " +jsonDF.count())
    println("JSON Schema")
    jsonDF.printSchema()

  val customerDF = spark.read.csv("src/main/resources/people.csv")
    customerDF.printSchema()

    val filterDF = customerDF.select("_c0","_c1","_c2")
      .groupBy("firstName").count()
      .filter(customerDF(" ") === (" "))
      .where("")
    filterDF.show()
  }

}
