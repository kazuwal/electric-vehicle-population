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
from pyspark.sql.functions import from_json, col, explode, explode_outer, stack, lit
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

        f = open("/opt/app/rows.json", "r")

        raw = json.load(f)

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
                StructField("format", StructType([StructField("align", StringType())])),
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

        rows_schema = StructType(
            [
                StructField("sid", StringType()),
                StructField("id", StringType()),
                StructField("position", StringType()),
                StructField("created_at", StringType()),
                StructField("created_meta", StringType()),
                StructField("updated_at", StringType()),
                StructField("updated_meta", StringType()),
                StructField("meta", StringType()),
                StructField("vin", StringType()),
                StructField("county", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType()),
                StructField("postal_code", StringType()),
                StructField("model_year", StringType()),
                StructField("make", StringType()),
                StructField("model", StringType()),
                StructField("electric_vehicle_type", StringType()),
                StructField(
                    "clean_alternative_fuel_vehicle_eligibility",
                    StringType(),
                ),
                StructField("electric_range", StringType()),
                StructField("base_msrp", StringType()),
                StructField("legislative_district", StringType()),
                StructField("dol_vehicle_id", StringType()),
                StructField("vehicle_location", StringType()),
                StructField("electric_utility", StringType()),
                StructField("2020_census_tract", StringType()),
                StructField("counties", StringType()),
                StructField("congressional_districts", StringType()),
                StructField("legislative_district_boundary", StringType()),
            ]
        )

        rows = self.spark.createDataFrame(raw["data"], schema=rows_schema)

        rows = rows.withColumn(
            "electric_range", rows["electric_range"].cast(IntegerType())
        )
        rows = rows.withColumn("base_msrp", rows["base_msrp"].cast(IntegerType()))
        rows = rows.withColumn(
            "congressional_districts",
            rows["congressional_districts"].cast(IntegerType()),
        )
        rows = rows.withColumn(
            "legislative_district_boundary",
            rows["legislative_district_boundary"].cast(IntegerType()),
        )

        dim_city = rows.select(col("city")).dropDuplicates()

        dim_city.write.option(
            "path", "file:/opt/bitnami/spark/spark-warehouse/dim_city"
        ).mode("overwrite").saveAsTable("dim_city")

        sales = self.spark.read.load(
            "/opt/app/sales.csv", format="csv", header=True, sep=","
        )

        x = sales.select(
            "Make",
            "Model",
            "Logo",
            stack(
                lit(96),
                lit("Jan 2012"),
                col("janv-12"),
                lit("Feb 2012"),
                col("Feb 2012"),
                lit("Mar 2012"),
                col("mars-12"),
                lit("Apr 2012"),
                col("Apr 2012"),
                lit("May 2012"),
                col("May 2012"),
                lit("Jun 2012"),
                col("juin-12"),
                lit("Jul 2012"),
                col("juil-12"),
                lit("Aug 2012"),
                col("Aug 2012"),
                lit("Sept 2012"),
                col("sept-12"),
                lit("Oct 2012"),
                col("oct-12"),
                lit("Nov 2012"),
                col("nov-12"),
                lit("Dec 2012"),
                col("Dec 2012"),
                lit("Jan 2013"),
                col("janv-13"),
                lit("Feb 2013"),
                col("Feb 2013"),
                lit("Mar 2013"),
                col("mars-13"),
                lit("Apr 2013"),
                col("Apr 2013"),
                lit("May 2013"),
                col("May 2013"),
                lit("Jun 2013"),
                col("juin-13"),
                lit("Jul 2013"),
                col("juil-13"),
                lit("Aug 2013"),
                col("Aug 2013"),
                lit("Sep 2013"),
                col("sept-13"),
                lit("Oct 2013"),
                col("oct-13"),
                lit("Nov 2013"),
                col("nov-13"),
                lit("Dec 2013"),
                col("Dec 2013"),
                lit("Jan 2014"),
                col("janv-14"),
                lit("Feb 2014"),
                col("Feb 2014"),
                lit("Mar 2014"),
                col("mars-14"),
                lit("Apr 2014"),
                col("Apr 2014"),
                lit("May 2014"),
                col("May 2014"),
                lit("Jun 2014"),
                col("juin-14"),
                lit("Jul 2014"),
                col("juil-14"),
                lit("Aug 2014"),
                col("Aug 2014"),
                lit("Sep 2014"),
                col("sept-14"),
                lit("Oct 2014"),
                col("oct-14"),
                lit("Nov 2014"),
                col("nov-14"),
                lit("Dec 2014"),
                col("Dec 2014"),
                lit("Jan 2015"),
                col("janv-15"),
                lit("Feb 2015"),
                col("Feb 2015"),
                lit("Mar 2015"),
                col("mars-15"),
                lit("Apr 2015"),
                col("Apr 2015"),
                lit("May 2015"),
                col("May 2015"),
                lit("Jun 2015"),
                col("juin-15"),
                lit("Jul 2015"),
                col("juil-15"),
                lit("Aug 2015"),
                col("Aug 2015"),
                lit("Sep 2015"),
                col("sept-15"),
                lit("Oct 2015"),
                col("oct-15"),
                lit("Nov 2015"),
                col("nov-15"),
                lit("Dec 2015"),
                col("Dec 2015"),
                lit("Jan 2016"),
                col("janv-16"),
                lit("Feb 2016"),
                col("Feb 2016"),
                lit("Mar 2016"),
                col("mars-16"),
                lit("Apr 2016"),
                col("Apr 2016"),
                lit("May 2016"),
                col("May 2016"),
                lit("Jun 2016"),
                col("juin-16"),
                lit("Jul 2016"),
                col("juil-16"),
                lit("Aug 2016"),
                col("Aug 2016"),
                lit("Sep 2016"),
                col("sept-16"),
                lit("Oct 2016"),
                col("oct-16"),
                lit("Nov 2016"),
                col("nov-16"),
                lit("Dec 2016"),
                col("Dec 2016"),
                lit("Jan 2017"),
                col("janv-17"),
                lit("Feb 2017"),
                col("Feb 2017"),
                lit("Mar 2017"),
                col("mars-17"),
                lit("Apr 2017"),
                col("Apr 2017"),
                lit("May 2017"),
                col("May 2017"),
                lit("Jun 2017"),
                col("juin-17"),
                lit("Jul 2017"),
                col("juil-17"),
                lit("Aug 2017"),
                col("Aug 2017"),
                lit("Sep 2017"),
                col("sept-17"),
                lit("Oct 2017"),
                col("oct-17"),
                lit("Nov 2017"),
                col("nov-17"),
                lit("Dec 2017"),
                col("Dec 2017"),
                lit("Jan 2018"),
                col("janv-18"),
                lit("Feb 2018"),
                col("Feb 2018"),
                lit("Mar 2018"),
                col("mars-18"),
                lit("Apr 2018"),
                col("Apr 2018"),
                lit("May 2018"),
                col("May 2018"),
                lit("Jun 2018"),
                col("juin-18"),
                lit("Jul 2018"),
                col("juil-18"),
                lit("Aug 2018"),
                col("Aug 2018"),
                lit("Sep 2018"),
                col("sept-18"),
                lit("Oct 2018"),
                col("oct-18"),
                lit("Nov 2018"),
                col("nov-18"),
                lit("Dec 2018"),
                col("Dec 2018"),
                lit("Jan 2019"),
                col("janv-19"),
                lit("Feb 2019"),
                col("Feb 2019"),
                lit("Mar 2019"),
                col("mars-19"),
                lit("Apr 2019"),
                col("Apr 2019"),
                lit("May 2019"),
                col("May 2019"),
                lit("Jun 2019"),
                col("juin-19"),
                lit("Jul 2019"),
                col("juil-19"),
                lit("Aug 2019"),
                col("Aug 2019"),
                lit("Sep 2019"),
                col("sept-19"),
                lit("Oct 2019"),
                col("oct-19"),
                lit("Nov 2019"),
                col("nov-19"),
                lit("Dec 2019"),
                col("Dec 2019"),
            ),
        )

        x.show()

def main():
    pass


if __name__ == "__main__":

    conf = SparkConf()
    conf.setAll(
        [
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
