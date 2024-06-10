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

        self.spark.sql("create schema if not exists stg")

        self.spark.sql("use stg")

        stg_ev_registration.write.option("path", f"{warehouse}/stg").mode(
            "overwrite"
        ).saveAsTable("ev_registration")

        self.spark.sql("select * from stg.ev_registration").show(5)

        int_ev_registration = self.spark.sql(
            """
            select
            sid,
            id,
            position,
            created_at,
            created_meta,
            updated_at,
            updated_meta,
            meta,
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

        self.spark.sql("create schema if not exists int")

        self.spark.sql("use int")

        int_ev_registration.write.option("path", f"{warehouse}/int").mode(
            "overwrite"
        ).saveAsTable("ev_registration")

        self.spark.sql("select * from int.ev_registration").show(5)

        start_date, end_date = "2020-01-01", "2030-01-01"

        df_date = spark.sql(
            f"select sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as seq"
        )

        df_date = df_date.select(explode(col("seq")).alias("date"))

        dim_day = (
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
            .withColumn(
                "FirstDayOfQuarter",
                expr(
                    "case when month(date) in (1,2,3) then date_sub(date_add(date, 1-day(date)), 0) "
                    + "when month(date) in (4,5,6) then date_sub(date_add(date, 1-day(date) + 90 - mod(dayofyear(date), 90)), 0) "
                    + "when month(date) in (7,8,9) then date_sub(date_add(date, 1-day(date) + 181 - mod(dayofyear(date), 91)), 0) "
                    + "else date_sub(date_add(date, 1-day(date) + 273 - mod(dayofyear(date), 92)), 0) end"
                ),
            )
            .withColumn(
                "LastDayOfQuarter",
                expr(
                    "case when month(date) in (1,2,3) then date_sub(date_add(date, 1-day(add_months(date, 3))), 1) "
                    + "when month(date) in (4,5,6) then date_sub(date_add(date, 1-day(add_months(date, 6))), 1) "
                    + "when month(date) in (7,8,9) then date_sub(date_add(date, 1-day(add_months(date, 9))), 1) "
                    + "else date_sub(date_add(date, 1-day(add_months(date, 12))), 1) end"
                ),
            )
            .withColumn(
                "IsLeapYear",
                when(
                    expr(
                        "mod(year(date), 4) == 0 and (mod(year(date), 100) != 0 or mod(year(date), 400) == 0)"
                    ),
                    lit(1),
                ).otherwise(lit(0)),
            )
            .withColumn(
                "Season",
                when(col("Month").isin(12, 1, 2), lit("Winter"))
                .when(col("Month").isin(3, 4, 5), lit("Spring"))
                .when(col("Month").isin(6, 7, 8), lit("Summer"))
                .otherwise(lit("Fall")),
            )
        )

        fiscal_year_start_month = 4  # For example, fiscal year starts in April

        dim_day = (
            dim_day.withColumn(
                "FiscalYear",
                when(
                    month(col("date")) >= fiscal_year_start_month, year(col("date")) + 1
                ).otherwise(year(col("date"))),
            )
            .withColumn(
                "FiscalQuarter",
                expr(
                    f"ceil((month(date) - {fiscal_year_start_month} + 12) % 12 / 3) + 1"
                ),
            )
            .withColumn(
                "FiscalMonth",
                expr(f"(month(date) - {fiscal_year_start_month} + 12) % 12 + 1"),
            )
            .withColumn(
                "FirstDayOfFiscalYear",
                expr(
                    f"case when month(date) >= {fiscal_year_start_month} then to_date(concat(year(date) + 1, '-{fiscal_year_start_month}-01'))"
                    + f"else to_date(concat(year(date), '-{fiscal_year_start_month}-01')) end"
                ),
            )
            .withColumn(
                "LastDayOfFiscalYear",
                expr(
                    f"date_sub(add_months(to_date(concat(FiscalYear, '-{fiscal_year_start_month}-01')), 12), 1)"
                ),
            )
            .withColumn(
                "FirstDayOfFiscalQuarter",
                expr(
                    f"case when month(date) in ({fiscal_year_start_month},{fiscal_year_start_month+1},{fiscal_year_start_month+2}) then to_date(concat(year(date), '-{fiscal_year_start_month}-01'))"
                    + f"when month(date) in ({fiscal_year_start_month+3},{fiscal_year_start_month+4},{fiscal_year_start_month+5}) then to_date(concat(year(date), '-{fiscal_year_start_month+3}-01'))"
                    + f"when month(date) in ({fiscal_year_start_month+6},{fiscal_year_start_month+7},{fiscal_year_start_month+8}) then to_date(concat(year(date), '-{fiscal_year_start_month+6}-01'))"
                    + f"else to_date(concat(year(date), '-{fiscal_year_start_month+9}-01')) end"
                ),
            )
            .withColumn(
                "LastDayOfFiscalQuarter",
                expr("date_sub(add_months(FirstDayOfFiscalQuarter, 3), 1)"),
            )
        )

        dim_day.cache()

        dim_week = dim_day.where(col("DayOfWeek").isin(1))

        dim_month = dim_day.where(col("DayOfMonth").isin(1))

        dim_day = dim_day.where(~col("DayOfWeek").isin(1))

        dim_day = dim_day.where(~col("DayOfMonth").isin(1))

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
