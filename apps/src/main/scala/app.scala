package upm.bd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import upm.bd.Preprocessing

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
    raw_df.printSchema()

    val preprocess_df = new Preprocessing().transform(raw_df)

    preprocess_df.show(6)
    preprocess_df.printSchema()
    // preprocess_df.write.format("csv").save("data/stage/preprocess-dataset.csv")

    // preprocess_df.select(
    //   preprocess_df.columns.map(
    //       c => count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)
    //         ).alias(c)
    //     ): _*
    //   ).show()

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
