package upm.bd

import org.apache.spark.ml.{Transformer, Pipeline}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}

import org.apache.spark.sql.{Dataset, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DropTransformer(columns: scala.Predef.String*) extends Transformer {
    val uid = Identifiable.randomUID("drop_transformer")

    override def transform(dataset: Dataset[_]): DataFrame = {
        dataset.drop(this.columns: _*)
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new DropTransformer(this.columns: _*), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}

class AssignTransformer(assignMap: Map[String, Function[DataFrame, Column]]) extends Transformer {
    val uid = Identifiable.randomUID("assign_transformer")

    override def transform(dataset: Dataset[_]): DataFrame = {
        this.assignMap.foldLeft(dataset.toDF())(
            (df, mapItem) => df.withColumn(mapItem._1, mapItem._2(df))
        )
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new AssignTransformer(this.assignMap), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}

class Preprocessing extends Transformer {
    val uid = Identifiable.randomUID("preprocessing")

    val assignMap = Map(
        "CRSElapsedTime" -> ((df: DataFrame) => 
            df("CRSElapsedTime").cast("integer")),
        "ArrDelay" -> ((df: DataFrame) => df("ArrDelay").cast("integer")),
        "DepDelay" -> ((df: DataFrame) => df("DepDelay").cast("integer")),
        "Distance" -> ((df: DataFrame) => df("Distance").cast("float")),
        "TaxiOut" -> ((df: DataFrame) => df("TaxiOut").cast("integer")),
        "Year" -> ((df: DataFrame) => col("Year").cast("integer")),
        "Month" -> ((df: DataFrame) => df("Month").cast("integer")),
        "DayofMonth" -> ((df: DataFrame) => df("DayofMonth").cast("integer")),
        "DayOfWeek" -> ((df: DataFrame) => df("DayOfWeek").cast("integer")),
        "DepTime" -> ((df: DataFrame) => df("DepTime").cast("integer")),
        "CRSDepTime" -> ((df: DataFrame) => df("CRSDepTime").cast("integer")),
        "CRSArrTime" -> ((df: DataFrame) => df("CRSArrTime").cast("integer")),
        "CRSElapsedTime" -> ((df: DataFrame) => df("CRSElapsedTime").cast("integer")),
        "Cancelled" -> ((df: DataFrame) => df("Cancelled").cast("integer")),
        "FlightNum" -> ((df: DataFrame) => df("FlightNum").cast("integer"))
    )
    
    val categoricalColumns = Array(
        "Origin",
        "Dest",
        "UniqueCarrier",
        "TailNum"
    )
    val indexCategoricalColumns =
        for (column <- this.categoricalColumns) yield s"Index$column"
    val vectorCategoricalColumns =
        for (column <- this.categoricalColumns) yield s"Vec$column"
                        
    val dropColumns = Array(
        // forbidden
        "ArrTime",
        "ActualElapsedTime",
        "AirTime",
        "TaxiIn",
        "Diverted",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
        // selected
        "CancellationCode"
    ) ++ categoricalColumns ++ indexCategoricalColumns

    val featuresColumns = Array(
        "Year",
        "Month",
        "DayofMonth",
        "DayOfWeek",
        "DepTime",
        "CRSDepTime",
        "CRSArrTime",
        "FlightNum",
        "CRSElapsedTime",
        "DepDelay",
        "Distance",
        "TaxiOut",
        "Cancelled",
    ) ++ vectorCategoricalColumns

    override def transform(dataset: Dataset[_]): DataFrame = {
        val assignTransformer = new AssignTransformer(this.assignMap)
        val stringIndexer = new StringIndexer()
            .setInputCols(this.categoricalColumns)
            .setOutputCols(this.indexCategoricalColumns)
        val oneHotEncoder = new OneHotEncoder()
            .setInputCols(this.indexCategoricalColumns)
            .setOutputCols(this.vectorCategoricalColumns)
        val dropTransformer = new DropTransformer(this.dropColumns: _*)
        val featureAssembler = new VectorAssembler()
            .setInputCols(this.featuresColumns)
            .setOutputCol("features")

        val pipeline = new Pipeline().setStages(Array(
            assignTransformer,
            stringIndexer,
            oneHotEncoder,
            dropTransformer,
            // NOTE The VectorAssembler doesn't work inside the pipeline
            // featureAssembler
        ))

        val _dataset = pipeline.fit(dataset).transform(dataset)
        featureAssembler.transform(_dataset)
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new Preprocessing(), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}
