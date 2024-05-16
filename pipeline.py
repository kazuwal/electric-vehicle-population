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
from pyspark.sql.functions import from_json, col, explode


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

        cols = raw.select("meta.view.columns").withColumn("columns", explode("columns"))

        cols.select(
            col("columns.id"),
            col("columns.name"),
            col("columns.dataTypeName"),
            col("columns.fieldName"),
            col("columns.position"),
            col("columns.renderTypeName"),
            col("columns.format"),
        ).show()


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
