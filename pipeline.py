from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    ArrayType,
    MapType,
)
import datetime
from pyspark.sql.functions import from_json, col, explode, explode_outer


class Job:
    def __init__(self, spark):
        self._spark = spark

    @property
    def spark(self):
        return self._spark

    @spark.getter
    def spark(self):
        return self._spark

    def run(self, opts=None):
        if opts is None:
            opts = {}

        raw = self.spark.read.option("multiline", True).load("rows.json", format="json")

        df = raw.select("meta.view.columns")

        df = df.withColumn("columns", explode("columns"))

        df = df.select(
            col("columns.computationStrategy.parameters.primary_key"),
            col("columns.computationStrategy.parameters.region"),
            col("columns.computationStrategy.source_columns"),
            col("columns.dataTypeName").alias("data_type_name"),
            col("columns.description"),
            col("columns.fieldName").alias("field_name"),
            col("columns.flags"),
            col("columns.format.align"),
            col("columns.id"),
            col("columns.name"),
            col("columns.position"),
            col("columns.renderTypeName").alias("render_type_name"),
            col("columns.tableColumnId").alias("table_column_id"),
        )

        df = df.withColumn("source_columns", explode_outer("source_columns"))
        df = df.withColumn("flags", explode_outer("flags"))

        df.show()


def main():
    pass


if __name__ == "__main__":

    conf = SparkConf()
    conf.setAll(
        [
            ("spark.master", "local[*]"),
            ("spark.app.name", "electric-vehicle-population"),
            ("spark.sql.debug.maxToStringFields", 1000),
        ]
    )
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    opts = {}

    job = Job(spark)

    job.run(opts)

    main()
