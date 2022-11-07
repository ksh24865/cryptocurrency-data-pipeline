from argparse import ArgumentParser
from functools import partial

from pyspark.sql import DataFrame

from spark.app.abc.base_cryptocurrency import SparkBaseAppCryptocurrency
from spark.transformation_factory.cryptocurrency_json_to_delta import (
    TransformationFactoryCryptocurrencyJsonToDelta,
)
from spark.utils.spark_session import get_spark_session
from spark.utils.top_list import TOP_SYMBOL_LIST_BY_MARKET_CAP

TRANSFORMATION = (
    TransformationFactoryCryptocurrencyJsonToDelta().build()
)  # pragma: no cover


class SparkAppCryptocurrencyJsonToDelta(SparkBaseAppCryptocurrency):  # pragma: no cover
    transformation = TRANSFORMATION

    def get_arg_parser(self) -> ArgumentParser:
        arg_parser = super().get_arg_parser()
        return arg_parser

    def get_src_path_(self, symbol: str) -> str:
        path_prefix = self.get_path_prefix()

        return f"{path_prefix}/json/{symbol}/*"

    def get_dest_path_(self, symbol: str) -> str:
        path_prefix = self.get_path_prefix()
        return f"{path_prefix}/delta/{symbol}"

    def read(self, path: str) -> DataFrame:
        return self.spark_session.read.format("json").load(path)

    def write(self, path: str, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path)
        )

    def run(self) -> None:
        arg_parser = self.get_arg_parser()
        args = arg_parser.parse_args()
        for symbol in TOP_SYMBOL_LIST_BY_MARKET_CAP:
            src_path = self.get_src_path_(symbol)
            dest_path = self.get_dest_path_(symbol)
            pure_enrichment = self.purify_enrichment(args)

            read_for_this_session = partial(self.read, src_path)
            write_for_this_session = partial(self.write, dest_path)
            enrich_for_this_session = partial(self.enrich, pure_enrichment)

            src_df = read_for_this_session()
            transformed_df = self.transform(src_df)
            enriched_df = enrich_for_this_session(transformed_df)
            write_for_this_session(enriched_df)


if __name__ == "__main__":  # pragma: no cover
    with get_spark_session("SparkAppCryptocurrencyJsonToDelta") as spark_session:
        spark_app_cryptocurrency_json_to_delta = SparkAppCryptocurrencyJsonToDelta(
            spark_session=spark_session,
        )
        spark_app_cryptocurrency_json_to_delta.run()
