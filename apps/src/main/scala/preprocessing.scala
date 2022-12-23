package upm.bd

import org.apache.spark.ml.{Transformer, Pipeline}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable

import org.apache.spark.sql.{Dataset, DataFrame}
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

class XPreprocessing extends Transformer {
    val uid = Identifiable.randomUID("x_preprocessing")

    val dropColumns = List(
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
    )

    override def transform(dataset: Dataset[_]): DataFrame = {
        val dropTransformer = new DropTransformer(this.dropColumns: _*)
        val pipeline = new Pipeline().setStages(Array(
            dropTransformer
        ))
        pipeline.fit(dataset).transform(dataset)
    }

    override def copy(extra: ParamMap): Transformer =
        copyValues(new XPreprocessing(), extra)

    override def transformSchema(schema: StructType): StructType = {
        StructType(schema.fields)
    }
}
