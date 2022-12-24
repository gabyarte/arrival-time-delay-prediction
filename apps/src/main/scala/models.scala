package upm.bd

import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.{Estimator, Pipeline}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, CrossValidatorModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.sql.{Dataset, DataFrame, Column}

// class LinearRegressionModel extends Estimator {
//     val linearRegression = new LinearRegression()
//         .setFeaturesCol("features")
//         .setLabelCol("ArrDelay")
//         .setMaxIter(10)
//         .setRegParam(0.3)
//         .setElasticNetParam(0.8)

//     override def fit() {
        
//     }
// }

class LinearRegressionHyperTuningModel {
    val linearRegression = new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("ArrDelay")
    val paramGrid = new ParamGridBuilder()
        .addGrid(linearRegression.regParam, Array(0.3, 0.2, 0.1, 0.01, 0.001))
        .addGrid(linearRegression.elasticNetParam, Array(1.0, 0.8, 0.75, 0.5, 0.25, 0.20, 0.0))
        // .addGrid(linearRegression.standardization, Array(true, false))
        .build()
    val crossValidation = new CrossValidator()
        .setEstimator(linearRegression)
        .setEvaluator(new RegressionEvaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(5)
        .setParallelism(2)
    var model: CrossValidatorModel = null

    def fit(training: Dataset[_]): LinearRegressionHyperTuningModel = {
        this.model = this.crossValidation.fit(training)
        this
    }

    def transform(dataset: Dataset[_]) {
        this.model.transform(dataset)
    }
}
