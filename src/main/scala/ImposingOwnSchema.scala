
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

  object ImposingOwnSchema {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .master("local")
        .appName("Imposing Own Schema on a Data Frame")
        .getOrCreate()

      val nameDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("src/main/resources/people.csv")

      println("Schema Inferred by Spark")
      nameDF.printSchema()

      //id,firstName,middleName,lastName,gender,birthDate,ssn,salary
      val ownSchema = StructType(
        StructField("id",LongType,true)::
          StructField("firstName", StringType, true)::
          StructField("middleName", StringType, true)::
          StructField("lastName", StringType, true)::
          StructField("gender", StringType, true)::
          StructField("birthDate", LongType, true)::
          StructField("ss", LongType, true)::
          StructField("salary", LongType, true):: Nil )

      val nameWithOwnSchema = spark.read
        .option("header", "true")
        .schema(ownSchema)
        .csv("src/main/resources/people.csv")

      println("Custom Schema Imposed with StructType")
      nameWithOwnSchema.printSchema()
    }

  }

