package upm.bd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import upm.bd.XPreprocessing

object App {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("Arrival delay time prediction")
      .setMaster("spark://spark-master:7077")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Preprocessing")
      .getOrCreate()

    import spark.implicits._

    val raw_df = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("data/raw/2008.csv")

    raw_df.show(6)

    val X = new XPreprocessing().transform(raw_df)

    X.show(6)

    // df.select(
    //   df.columns.map(c =>
    //     count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)).alias(c)
    //   ): _*
    // ).show()

    // val df0 =
    //   df.filter(df("Cancelled") === "0").drop("Cancelled", "CancellationCode")
    // val df1 = df0
    //   .withColumn("CRSElapsedTime", df0("CRSElapsedTime").cast("integer"))
    //   .withColumn("ArrDelay", df0("ArrDelay").cast("integer"))
    //   .withColumn("DepDelay", df0("DepDelay").cast("integer"))
    //   .withColumn("Distance", df0("Distance").cast("integer"))
    //   .withColumn("TaxiOut", df0("TaxiOut").cast("integer"))
    //   .withColumn("DepTime", regexp_replace(df0("DepTime"), "..$", ":$0"))
    //   .withColumn("CRSDepTime", regexp_replace(df0("CRSDepTime"), "..$", ":$0"))
    //   .withColumn("CRSArrTime", regexp_replace(df0("CRSArrTime"), "..$", ":$0"))

    // val df2 = df1
    //   .withColumn(
    //     "CRSDepTime",
    //     to_timestamp(
    //       concat(
    //         df1("Year"),
    //         lit('-'),
    //         df1("Month"),
    //         lit('-'),
    //         df1("DayofMonth"),
    //         lit(' '),
    //         df1("CRSDepTime")
    //       )
    //     )
    //   )
    //   .withColumn(
    //     "CRSArrTime",
    //     to_timestamp(
    //       concat(
    //         df1("Year"),
    //         lit('-'),
    //         df1("Month"),
    //         lit('-'),
    //         df1("DayofMonth"),
    //         lit(' '),
    //         df1("CRSArrTime")
    //       )
    //     )
    //   )

    // val df3 = df2
    //   .drop("Year", "Month", "DayofMonth")
    //   .withColumn(
    //     "DepTime",
    //     to_timestamp(
    //       from_unixtime(
    //         unix_timestamp(df2("CRSDepTime")) + (df2("DepDelay") * 60),
    //         "yyyy-MM-dd HH:mm:ss"
    //       )
    //     )
    //   )

    // df3.show(6)
    // df3.printSchema()

    // val assembler = new VectorAssembler()
    //   .setInputCols(
    //     Array(
    //       "DayOfWeek",
    //       "DepTime",
    //       "CRSDepTime",
    //       "CRSArrTime",
    //       "UniqueCarrier",
    //       "FlightNum",
    //       "TailNum",
    //       "CRSElapsedTime",
    //       "ArrDelay",
    //       "DepDelay",
    //       "Origin",
    //       "Dest",
    //       "Distance",
    //       "TaxiOut"
    //     )
    //   )
    //   .setOutputCol("features")
    // val output = assembler.transform(df3)
    // output.show(truncate = false)

    // all feature must be numeric
    // val lr = new LinearRegression()
    //   .setFeaturesCol("features")
    //   .setLabelCol("ArrDelay")
    //   .setMaxIter(10)
    //   .setRegParam(0.3)
    //   .setElasticNetParam(0.8)

    // val lrModel = lr.fit(output)
    // println(s"Coefficients: ${lrModel.coefficients}")
    // println(s"Intercept: ${lrModel.intercept}")
    // val trainingSummary = lrModel.summary
    // println(s"numIterations: ${trainingSummary.totalIterations}")
    // println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    // trainingSummary.residuals.show()
    // println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    // println(s"r2: ${trainingSummary.r2}")

  }
}
