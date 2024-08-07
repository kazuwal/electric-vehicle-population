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
    expr,
    when,
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

        staging = "file:/opt/bitnami/spark/spark-warehouse/stg"
        integration = "file:/opt/bitnami/spark/spark-warehouse/int"

        """
            create schemas
        """

        self.spark.sql("create schema if not exists stg")
        self.spark.sql("create schema if not exists int")

        """
            create date dimension tables
        """

        self.spark.sql("use stg")

        start_date, end_date = "2020-01-01", "2030-01-01"

        df_date = self.spark.sql(
            f"select sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as seq"
        )

        df_date = df_date.select(explode(col("seq")).alias("date"))

        stg_day = (
            df_date.withColumn(
                "DateKey", date_format(col("date"), "yyyyMMdd").cast("int")
            )
            .withColumn("FullDate", col("date"))
            .withColumn("DayOfMonth", dayofmonth(col("date")))
            .withColumn(
                "DaySuffix",
                when(col("DayOfMonth").isin(1, 21, 31), lit("st"))
                .when(col("DayOfMonth").isin(2, 22), lit("nd"))
                .when(col("DayOfMonth").isin(3, 23), lit("rd"))
                .otherwise(lit("th")),
            )
            .withColumn("DayName", date_format(col("date"), "EEEE"))
            .withColumn("DayOfWeek", dayofweek(col("date")))
            .withColumn(
                "IsWeekend",
                when(dayofweek(col("date")).isin(1, 7), lit(1)).otherwise(lit(0)),
            )
            .withColumn("WeekOfYear", weekofyear(col("date")))
            .withColumn("Month", month(col("date")))
            .withColumn("MonthName", date_format(col("date"), "MMMM"))
            .withColumn("Quarter", quarter(col("date")))
            .withColumn("Year", year(col("date")))
            .withColumn("DayOfYear", dayofyear(col("date")))
            .withColumn("WeekOfMonth", expr("ceil(dayofmonth(date) / 7)"))
            .withColumn(
                "FirstDayOfMonth", expr("date_sub(date_add(date, 1-day(date)), 0)")
            )
            .withColumn(
                "LastDayOfMonth",
                expr("date_sub(date_add(date, 1-day(add_months(date, 1))), 1)"),
            )
        )

        stg_day.cache()

        stg_week = stg_day.where(col("DayOfWeek").isin(1))

        stg_month = stg_day.where(col("DayOfMonth").isin(1))

        stg_day = stg_day.where(
            (~col("DayOfWeek").isin(1)) & (~col("DayOfMonth").isin(1))
        )

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

        stg_ev_registration_schema = StructType(
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

        stg_ev_registration = self.spark.createDataFrame(
            raw["data"], schema=stg_ev_registration_schema
        )

        stg_day.write.option("path", f"{staging}/day").mode("overwrite").saveAsTable(
            "day"
        )

        stg_week.write.option("path", f"{staging}/week").mode("overwrite").saveAsTable(
            "week"
        )

        stg_month.write.option("path", f"{staging}/month").mode(
            "overwrite"
        ).saveAsTable("month")

        stg_ev_registration.write.option("path", f"{staging}/ev_registration").mode(
            "overwrite"
        ).saveAsTable("ev_registration")

        self.spark.sql("use int")

        int_day = self.spark.sql(
            """
                select
                    date,
                    DateKey,
                    FullDate,
                    DayOfMonth,
                    DaySuffix,
                    DayName,
                    DayOfWeek,
                    IsWeekend,
                    WeekOfYear,
                    Month,
                    MonthName,
                    Quarter,
                    Year,
                    DayOfYear,
                    WeekOfMonth,
                    FirstDayOfMonth,
                    LastDayOfMonth
                from stg.day
        """
        )

        int_day.show(10)

        int_week = self.spark.sql(
            """
                select
                    date,
                    DateKey,
                    FullDate,
                    WeekOfYear,
                    Month,
                    MonthName,
                    Quarter,
                    Year,
                    WeekOfMonth
                from stg.week
        """
        )

        int_week.show(10)

        int_month = self.spark.sql(
            """
                select
                    date,
                    DateKey,
                    FullDate,
                    Month,
                    MonthName,
                    Quarter,
                    Year
                from stg.month
        """
        )

        int_month.show(10)

        int_ev_registration = self.spark.sql(
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
            from stg.ev_registration
        """
        )

        int_day = self.spark.sql(
            """
            select * from stg.day
        """
        )

        int_ev_registration.write.option("path", f"{integration}/ev_registration").mode(
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
            from int.ev_registration er
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
                from int.ev_registration er
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
