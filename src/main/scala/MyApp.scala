package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object MyApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("My first Spark application")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .config("some option", "value")
      .getOrCreate()

    import spark.implicits._

    val raw_df = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("/home/pablo/Desktop/Big Data/data/2008.csv")

    val df = raw_df.drop(
      "ArrTime",
      "ActualElapsedTime",
      "AirTime",
      "TaxiIn",
      "Diverted",
      "CarrierDelay",
      "WeatherDelay",
      "NASDelay",
      "SecurityDelay",
      "LateAircraftDelay"
      // "FlightNum" not relevant(?)
    )
    df.withColumn("Year", df("Year").cast("integer"))
      .withColumn("Month", df("Month").cast("integer"))
      .withColumn("DayofMonth", df("DayofMonth").cast("integer"))
      .printSchema()

    df.show(1)
    // df.withColumn(
    //   "Date",
    //   to_timestamp(
    //     concat(df("DayofMonth"), lit('-'), df("Month"), lit('-'), df("Year")),
    //     "M-d-yyyy"
    //   )
    // ).show(1)

  }
}
