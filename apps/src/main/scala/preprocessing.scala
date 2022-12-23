package upm.bd

import org.apache.spark.ml.{Transformer, Pipeline}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable

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

class XPreprocessing extends Transformer {
    val uid = Identifiable.randomUID("x_preprocessing")

    val dropColumns = List(
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
    )

    val assignMap = Map(
        "CRSElapsedTime" -> ((df: DataFrame) => 
            df("CRSElapsedTime").cast("integer")),
        "ArrDelay" -> ((df: DataFrame) => df("ArrDelay").cast("integer")),
        "DepDelay" -> ((df: DataFrame) => df("DepDelay").cast("integer")),
        "Distance" -> ((df: DataFrame) => df("Distance").cast("integer")),
        "TaxiOut" -> ((df: DataFrame) => df("TaxiOut").cast("integer")),
        "DepTime" -> ((df: DataFrame) => 
            regexp_replace(df("DepTime"), "..$", ":$0")),
        "CRSDepTime" -> ((df: DataFrame) => 
            regexp_replace(df("CRSDepTime"), "..$", ":$0")),
        "CRSArrTime" -> ((df: DataFrame) => 
            regexp_replace(df("CRSArrTime"), "..$", ":$0"))
    )

    override def transform(dataset: Dataset[_]): DataFrame = {
        val dropTransformer = new DropTransformer(this.dropColumns: _*)
        val assignTransformer = new AssignTransformer(this.assignMap)

        val pipeline = new Pipeline().setStages(Array(
            dropTransformer,
            assignTransformer
        ))
        pipeline.fit(dataset).transform(dataset)
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new XPreprocessing(), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}
