package upm.bd

import java.io.{FileWriter, File}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame, Column}
import org.apache.spark.sql.functions._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, FMRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator

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

    // val model = new LinearRegressionHyperTuningModel().fit(preprocess_df)
    // val bestModel = model.model.bestModel.asInstanceOf[LinearRegressionModel]

    // println(s"Average metric: ${model.model.avgMetrics}")

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = preprocess_df.randomSplit(Array(0.7, 0.3))

    // val lr = new LinearRegression()
    //   .setMaxIter(10)
    //   .setRegParam(0.3)
    //   .setElasticNetParam(0.8)
    //   .setFeaturesCol("features")
    //   .setLabelCol("ArrDelay")
    
    // val lrModel = lr.fit(preprocess_df)
    // val trainingSummary = lrModel.summary

    // val pw = new FileWriter(new File("data/lrResults.txt" ))
    // pw.write(s"Coefficients: ${lrModel.coefficients}")
    // pw.write(s"Coefficients: ${lrModel.coefficients}")
    // pw.write(s"Intercept: ${lrModel.intercept}")
    // pw.write(s"numIterations: ${trainingSummary.totalIterations}")
    // pw.write(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    // trainingSummary.residuals.show()
    // pw.write(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    // pw.write(s"r2: ${trainingSummary.r2}")
    // pw.close()

    // // Train a GBT model.
    // val gbt = new GBTRegressor()
    //   .setLabelCol("ArrDelay")
    //   .setFeaturesCol("features")
    //   .setMaxIter(5)
    // // Train model. This also runs the indexer.
    // val gbtModel = gbt.fit(trainingData)
    // // Make predictions.
    // val gbtPredictions = gbtModel.transform(testData)
    // // Select example rows to display.
    // gbtPredictions.select("prediction", "ArrDelay", "features").show(5)
    // // Select (prediction, true label) and compute test error.
    // val gbtEvaluator = new RegressionEvaluator()
    //   .setLabelCol("ArrDelay")
    //   .setPredictionCol("prediction")
    //   .setMetricName("rmse")
    // val rmse = gbtEvaluator.evaluate(gbtPredictions)
    // println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    // println(s"Learned regression GBT model:\n ${model.toDebugString}")

    // Train a FM model.
    val fm = new FMRegressor()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("features")
      .setStepSize(0.001)
    // Train model.
    val fmModel = fm.fit(trainingData)
    // Make predictions.
    val fmPredictions = fmModel.transform(testData)
    // Select example rows to display.
    fmPredictions.select("prediction", "ArrDelay", "features").show(5)
    // Select (prediction, true label) and compute test error.
    val fmEvaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = fmEvaluator.evaluate(fmPredictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    println(s"Factors: ${fmModel.factors} Linear: ${fmModel.linear} " +
      s"Intercept: ${fmModel.intercept}")
  }
}
