package upm.bd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame, Column}
import org.apache.spark.sql.functions._

import org.apache.spark.ml.regression.LinearRegressionModel

import org.apache.log4j.{Level, Logger}

import upm.bd.Preprocessing
import upm.bd.LinearRegressionHyperTuningModel

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

    // raw_df.select(
    //   raw_df.columns.map(
    //       c => count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)
    //         ).alias(c)
    //     ): _*
    //   ).show()

    val preprocess_df = new Preprocessing().transform(raw_df)

    def countCols(columns:Array[String]):Array[Column]={
      columns.map(c=>{
        count(when(col(c).isNull,c)).alias(c)
      })
    }
    preprocess_df.select(countCols(preprocess_df.columns):_*).show()

    preprocess_df.show(6)
    preprocess_df.printSchema()
    // preprocess_df.write.format("csv").save("data/stage/preprocess-dataset.csv")

    val model = new LinearRegressionHyperTuningModel().fit(preprocess_df)
    // val bestModel = model.model.bestModel.asInstanceOf[LinearRegressionModel]

    // println(s"Average metric: ${model.model.avgMetrics}")

    // println(s"Coefficients: ${bestModel.coefficients}")
    // println(s"Intercept: ${bestModel.intercept}")

    // val trainingSummary = bestModel.summary
    // println(s"numIterations: ${trainingSummary.totalIterations}")
    // println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    // trainingSummary.residuals.show()
    // println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    // println(s"r2: ${trainingSummary.r2}")
  }
}
