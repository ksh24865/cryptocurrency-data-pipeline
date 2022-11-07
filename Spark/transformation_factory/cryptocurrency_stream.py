from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame  # pragma: no cover
from pyspark.sql.functions import col, from_json, lit  # pragma: no cover
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark._typing import Transformation


class TransformationFactoryCryptocurrencyStream:  # pragma: no cover
    @staticmethod
    def transform_to_cast_string(df: DataFrame) -> DataFrame:
        return df.selectExpr("CAST(value AS STRING)")

    @staticmethod
    def transform_to_parse_json_string(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "value",
            from_json(
                col("value"),
                StructType(
                    [
                        StructField("symbol", StringType()),
                        StructField("close", DoubleType()),
                    ]
                ),
            ),
        ).select("value.*")

    @staticmethod
    def transform_to_add_time(df: DataFrame) -> DataFrame:
        return df.withColumn("time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    @staticmethod
    def transform_to_fill_schema(df: DataFrame) -> DataFrame:
        schema = [
            "time",
            "high",
            "low",
            "open",
            "close",
            "volumefrom",
            "volumeto",
            "symbol",
        ]
        return reduce(
            lambda acc_df, column: acc_df.withColumn(column, lit(0.0)),
            [column for column in schema if column not in df.schema],
            df,
        )

    def build(self) -> Transformation:
        return (
            type(self).transform_to_cast_string,
            type(self).transform_to_parse_json_string,
            type(self).transform_to_add_time,
            type(self).transform_to_fill_schema,
        )
