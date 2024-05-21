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
import json


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

        with open("rows.json", "r") as f:
            raw = json.load(f)

            # raw = self.spark.read.option("multiline", True).load("rows.json", format="json")

            columns_schema = StructType(
                [
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("dataTypeName", StringType()),
                    StructField("description", StringType()),
                    StructField("fieldName", StringType()),
                    StructField("position", StringType()),
                    StructField("flags", ArrayType(StringType())),
                    StructField("renderTypeName", StringType()),
                    StructField("tableColumnId", StringType()),
                    StructField(
                        "computationStrategy",
                        StructType(
                            [
                                StructField("source_columns", ArrayType(StringType())),
                                StructField("type", StringType()),
                                StructField(
                                    "parameters",
                                    StructType(
                                        [
                                            StructField("region", StringType()),
                                            StructField("primary_key", StringType()),
                                        ]
                                    ),
                                ),
                            ]
                        ),
                    ),
                    StructField(
                        "format", StructType([StructField("align", StringType())])
                    ),
                ]
            )

            columns = self.spark.createDataFrame(
                raw["meta"]["view"]["columns"], schema=columns_schema
            )

            columns = columns.select(
                col("id"),
                col("name"),
                col("dataTypeName"),
                col("fieldName"),
                col("position"),
                col("flags"),
                col("renderTypeName"),
                col("tableColumnId"),
                col("computationStrategy"),
                col("description"),
                col("format"),
            )

            columns = (
                columns.select(
                    col("id"),
                    col("name"),
                    col("dataTypeName").alias("data_type_name"),
                    col("fieldName").alias("field_name"),
                    col("position"),
                    col("flags"),
                    col("renderTypeName").alias("render_type_name"),
                    col("tableColumnId").alias("table_column_id"),
                    col("computationStrategy.source_columns"),
                    col("computationStrategy.type"),
                    col("computationStrategy.parameters.region"),
                    col("computationStrategy.parameters.primary_key"),
                    col("description"),
                    col("format.align"),
                )
                .withColumn("flags", explode_outer("flags"))
                .withColumn("source_columns", explode_outer("source_columns"))
            )

            columns = columns.where("id == 583288609")

            columns.show()


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