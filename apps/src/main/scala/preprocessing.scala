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

class FilterTransformer(filterFunction: Function[DataFrame, Column]) extends Transformer {
    val uid = Identifiable.randomUID("filter_transformer")

    override def transform(dataset: Dataset[_]): DataFrame = {
        val df = dataset.toDF()
        df.filter(this.filterFunction(df))
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new FilterTransformer(this.filterFunction), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}

class Preprocessing extends Transformer {
    val uid = Identifiable.randomUID("preprocessing")

    val assignMap = Map(
        "CRSElapsedTime" -> ((df: DataFrame) => 
            df("CRSElapsedTime").cast("integer")),
        "ArrDelay" -> ((df: DataFrame) => df("ArrDelay").cast("integer").asInstanceOf[Column]),
        "DepDelay" -> ((df: DataFrame) => df("DepDelay").cast("integer")),
        "Distance" -> ((df: DataFrame) => df("Distance").cast("float")),
        "TaxiOut" -> ((df: DataFrame) => df("TaxiOut").cast("integer")),
        "Year" -> ((df: DataFrame) => df("Year").cast("integer")),
        "Month" -> ((df: DataFrame) => df("Month").cast("integer")),
        "DayofMonth" -> ((df: DataFrame) => df("DayofMonth").cast("integer")),
        "DayOfWeek" -> ((df: DataFrame) => df("DayOfWeek").cast("integer")),
        "DepTime" -> ((df: DataFrame) => df("DepTime").cast("integer")),
        "CRSDepTime" -> ((df: DataFrame) => df("CRSDepTime").cast("integer")),
        "CRSArrTime" -> ((df: DataFrame) => df("CRSArrTime").cast("integer")),
        "CRSElapsedTime" -> ((df: DataFrame) => df("CRSElapsedTime").cast("integer")),
        "Cancelled" -> ((df: DataFrame) => df("Cancelled").cast("integer")),
        "FlightNum" -> ((df: DataFrame) => df("FlightNum").cast("integer")),
    )

    val filterFunction = (df: DataFrame) => df("Cancelled") === "0"

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
        "Year",
        "CancellationCode",
        "Cancelled",
    ) ++ categoricalColumns ++ indexCategoricalColumns

    val featuresColumns = Array(
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
    ) ++ vectorCategoricalColumns

    override def transform(dataset: Dataset[_]): DataFrame = {
        val filterTransformer = new FilterTransformer(this.filterFunction)
        val assignTransformer = new AssignTransformer(this.assignMap)
        val stringIndexer = new StringIndexer()
            .setHandleInvalid("keep")
            .setInputCols(this.categoricalColumns)
            .setOutputCols(this.indexCategoricalColumns)
        val oneHotEncoder = new OneHotEncoder()
            .setInputCols(this.indexCategoricalColumns)
            .setOutputCols(this.vectorCategoricalColumns)
        val dropTransformer = new DropTransformer(this.dropColumns: _*)
        val featureAssembler = new VectorAssembler()
            .setInputCols(this.featuresColumns)
            .setOutputCol("features")

        var _dataset = filterTransformer.transform(dataset)
        _dataset = assignTransformer.transform(_dataset)
        _dataset = stringIndexer.fit(_dataset).transform(_dataset)
        _dataset = _dataset.na.fill("Unknown", Array("TailNum"))
                           .na.fill(0, Array("ArrDelay"))
                           .na.fill(0, Array("CRSElapsedTime"))
         _dataset = oneHotEncoder.fit(_dataset).transform(_dataset)
         _dataset = dropTransformer.transform(_dataset)
        featureAssembler.transform(_dataset)
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new Preprocessing(), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}
