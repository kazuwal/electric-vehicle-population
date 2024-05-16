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
import pyspark.sql.functions as func


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

        pass


def main():
    pass


if __name__ == "__main__":

    conf = SparkConf()
    conf.setAll(
        [
            ("spark.master", "local[*]"),
            ("spark.app.name", "electric-vehicle-population"),
        ]
    )
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    opts = {}

    job = Job(spark)

    job.run(opts)

    main()
