from functools import reduce

from pyspark.sql import DataFrame  # pragma: no cover
from pyspark.sql.functions import col, explode, from_unixtime  # pragma: no cover
from pyspark.sql.types import DoubleType

from spark._typing import Transformation

NEEDED_COLUMNS = ["time", "high", "low", "open", "close", "volumefrom", "volumeto"]
TO_BE_CASTED_COLUMNS = ["high", "low", "open", "close", "volumefrom", "volumeto"]


class TransformationFactoryCryptocurrencyJsonToDelta:  # pragma: no cover
    @staticmethod
    def transform_to_explode_data(df: DataFrame) -> DataFrame:
        return (
            df.select("Data.*")
            .withColumn("Data", explode(col("Data")))
            .select("Data.*")
        )

    @staticmethod
    def transform_to_parse_timestamp(df: DataFrame) -> DataFrame:
        return df.withColumn("time", from_unixtime(col("time")))

    @staticmethod
    def transform_to_cast_double(df: DataFrame) -> DataFrame:
        def cast_double(df: DataFrame, column_name: str) -> DataFrame:
            return df.withColumn(column_name, col(column_name).cast(DoubleType()))

        return reduce(cast_double, TO_BE_CASTED_COLUMNS, df)

    @staticmethod
    def transform_to_slice_by_needed_column(df: DataFrame) -> DataFrame:
        return df.select(NEEDED_COLUMNS)

    def build(self) -> Transformation:
        return (
            type(self).transform_to_explode_data,
            type(self).transform_to_parse_timestamp,
            type(self).transform_to_slice_by_needed_column,
            type(self).transform_to_cast_double,
        )
