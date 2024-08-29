import datetime
import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    explode,
    explode_outer,
    expr,
    first,
    from_json,
    lit,
    max,
    min,
    month,
    quarter,
    split,
    stack,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
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

        bronze = "file:/opt/bitnami/spark/spark-warehouse/bronze"
        silver = "file:/opt/bitnami/spark/spark-warehouse/silver"
        gold = "file:/opt/bitnami/spark/spark-warehouse/gold"

        """
            create schemas
        """

        self.spark.sql("create schema if not exists bronze")
        self.spark.sql("create schema if not exists silver")
        self.spark.sql("create schema if not exists gold")

        """
            create date dimension tables
        """

        self.spark.sql("use bronze")

        start_date, end_date = "2020-01-01", "2030-01-01"

        df_date = self.spark.sql(
            f"select sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as seq"
        )

        df_date = df_date.select(explode(col("seq")).alias("date"))

        bronze_day = (
            df_date.withColumn(
                "date_key", date_format(col("date"), "yyyyMMdd").cast("int")
            )
            .withColumn("day_of_month", dayofmonth(col("date")))
            .withColumn(
                "day_suffix",
                when(col("day_of_month").isin(1, 21, 31), lit("st"))
                .when(col("day_of_month").isin(2, 22), lit("nd"))
                .when(col("day_of_month").isin(3, 23), lit("rd"))
                .otherwise(lit("th")),
            )
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn(
                "is_weekend",
                when(dayofweek(col("date")).isin(1, 7), lit(1)).otherwise(lit(0)),
            )
            .withColumn("week_of_year", weekofyear(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("year", year(col("date")))
            .withColumn("day_of_year", dayofyear(col("date")))
            .withColumn("week_of_month", expr("ceil(dayofmonth(date) / 7)"))
            .withColumn(
                "first_day_of_month", expr("date_sub(date_add(date, 1-day(date)), 0)")
            )
            .withColumn(
                "last_day_of_month",
                expr("date_sub(date_add(date, 1-day(add_months(date, 1))), 1)"),
            )
        )

        bronze_day.cache()

        assert bronze_day.count() == 3654

        bronze_week = bronze_day.groupBy("year", "week_of_year").agg(
            min("date").alias("date"),  # Get the first date of the week
            first("date_key").alias("date_key"),
            first("month").alias("month"),
            first("month_name").alias("month_name"),
            first("quarter").alias("quarter"),
            first("week_of_month").alias("week_of_month"),
        )

        assert (
            bronze_week.count()
            == bronze_week.dropDuplicates(bronze_week.columns).count()
        )

        bronze_month = bronze_day.where(col("day_of_month").isin(1))

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

        bronze_ev_registration_schema = StructType(
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

        bronze_ev_registration = self.spark.createDataFrame(
            raw["data"], schema=bronze_ev_registration_schema
        )

        bronze_day.write.option("path", f"{bronze}/day").mode("overwrite").saveAsTable(
            "day"
        )

        bronze_week.write.option("path", f"{bronze}/week").mode(
            "overwrite"
        ).saveAsTable("week")

        bronze_month.write.option("path", f"{bronze}/month").mode(
            "overwrite"
        ).saveAsTable("month")

        bronze_ev_registration.write.option("path", f"{bronze}/ev_registration").mode(
            "overwrite"
        ).saveAsTable("ev_registration")

        self.spark.sql("use silver")

        silver_day = self.spark.sql(
            """
                select
                    date,
                    date_key,
                    day_of_month,
                    day_suffix,
                    day_name,
                    day_of_week,
                    is_weekend,
                    week_of_year,
                    month,
                    month_name,
                    quarter,
                    year,
                    day_of_year,
                    week_of_month,
                    first_day_of_month,
                    last_day_of_month
                from bronze.day
        """
        )

        silver_day.show(10)

        silver_week = self.spark.sql(
            """
                select
                    date,
                    date_key,
                    week_of_year,
                    month,
                    month_name,
                    quarter,
                    year,
                    week_of_month
                from bronze.week
        """
        )

        silver_week.show(10)

        silver_month = self.spark.sql(
            """
                select
                    date,
                    date_key,
                    month,
                    month_name,
                    quarter,
                    year
                from bronze.month
        """
        )

        silver_month.show(10)

        silver_ev_registration = self.spark.sql(
            """
            select
            vin,
            county,
            city,
            state,
            postal_code,
            model_year,
            make,
            model,
            electric_vehicle_type,
            clean_alternative_fuel_vehicle_eligibility,
            cast(electric_range as int),
            cast(base_msrp as int),
            legislative_district,
            dol_vehicle_id,
            vehicle_location,
            split(electric_utility, '\\\|\\\|?')[0] as electric_utility_0,
            split(electric_utility, '\\\|\\\|?')[1] as electric_utility_1,
            2020_census_tract,
            counties,
            cast(congressional_districts as int),
            cast(legislative_district_boundary as int)
            from bronze.ev_registration
        """
        )

        silver_day = self.spark.sql(
            """
            select * from bronze.day
        """
        )

        silver_ev_registration.write.option("path", f"{silver}/ev_registration").mode(
            "overwrite"
        ).saveAsTable("ev_registration")

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
            from silver.ev_registration er
        """
        )

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
                from silver.ev_registration er
            """
        )

        ev_sales_schema = StructType(
            [
                StructField("make", StringType()),
                StructField("model", StringType()),
                StructField("logo", StringType()),
                StructField("date", StringType()),
                StructField("sales", StringType()),
            ]
        )

        ev_sales = self.spark.read.load(
            f"{home}/ev_sales.csv",
            format="csv",
            header=True,
            sep=",",
            schema=ev_sales_schema,
        )


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
