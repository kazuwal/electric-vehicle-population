from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import datetime
import json

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
from pyspark.sql.functions import (
    from_json,
    col,
    explode,
    explode_outer,
    stack,
    lit,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    month,
    quarter,
    weekofyear,
    year,
    split,
)


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

        home = "/opt/app"
        warehouse = "file:/opt/bitnami/spark/spark-warehouse"

        f = open(f"{home}/ev_registration.json", "r")

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

        ev_registration_schema = StructType(
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

        ev_registration = self.spark.createDataFrame(
            raw["data"], schema=ev_registration_schema
        )

        ev_registration = ev_registration.withColumn(
            "electric_range", ev_registration["electric_range"].cast(IntegerType())
        )
        ev_registration = ev_registration.withColumn(
            "base_msrp", ev_registration["base_msrp"].cast(IntegerType())
        )
        ev_registration = ev_registration.withColumn(
            "congressional_districts",
            ev_registration["congressional_districts"].cast(IntegerType()),
        )
        ev_registration = ev_registration.withColumn(
            "legislative_district_boundary",
            ev_registration["legislative_district_boundary"].cast(IntegerType()),
        )

        ev_registration = ev_registration.withColumn(
            "electric_utilities", split(ev_registration["electric_utility"], r"\|\|?")
        )

        ev_registration = (
            ev_registration.withColumn(
                "electric_utility_0", ev_registration["electric_utilities"].getItem(0)
            )
            .withColumn(
                "electric_utility_1", ev_registration["electric_utilities"].getItem(1)
            )
            .drop("electric_utilities")
        )

        # create the ev reg staging table
        ev_registration.write.option("path", f"{warehouse}/stg_ev_registration").mode(
            "overwrite"
        ).saveAsTable("stg_ev_registration")

        start_date, end_date = "2020-01-01", "2023-12-31"

        df_date = spark.sql(
            f"select sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as seq"
        )

        df_date = df_date.select(explode(col("seq")).alias("date"))

        dim_date = (
            df_date.withColumn("year", year(col("date")))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn("week_of_year", weekofyear(col("date")))
            .withColumn("day_of_year", dayofyear(col("date")))
            .withColumn("is_weekend", (col("day_of_week").isin([1, 7])).cast("boolean"))
            .withColumn("date_str", date_format(col("date"), "yyyy-MM-dd"))
        )

        dim_location = self.spark.sql(
            """
            select distinct er.county,
                   er.city,
                   er.state,
                   er.postal_code,
                   er.counties,
                   er.congressional_districts,
                   er.legislative_district,
                   er.vehicle_location
            from stg_ev_registration er
        """
        )

        dim_location.show(5)

        dim_car = self.spark.sql(
            """
                select distinct er.make,
                       er.model,
                       er.model_year,
                       er.electric_vehicle_type,
                       er.clean_alternative_fuel_vehicle_eligibility,
                       er.electric_range,
                       er.base_msrp,
                       er.vin,
                       er.dol_vehicle_id
                from stg_ev_registration er
            """
        )

        dim_car.show(5)

        ev_sales_schema = StructType(
            [
                StructField("make", StringType()),
                StructField("model", StringType()),
                StructField("logo", StringType()),
                StructField("date", StringType()),
                StructField("sales", DoubleType()),
            ]
        )

        ev_sales = self.spark.read.load(
            f"{home}/ev_sales.csv",
            format="csv",
            header=True,
            sep=",",
            schema=ev_sales_schema,
        )

        ev_sales.write.option("path", f"{warehouse}/stg_ev_sales").mode(
            "overwrite"
        ).saveAsTable("stg_ev_sales")


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
