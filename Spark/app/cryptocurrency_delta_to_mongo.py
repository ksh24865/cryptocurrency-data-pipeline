from functools import partial

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from spark.app.abc.base_cryptocurrency import SparkBaseAppCryptocurrency
from spark.utils.db_access_info import (
    MONGO_DRIVER,
    MONGO_HOST,
    MONGO_PASSWORD,
    MONGO_PORT,
    MONGO_USER,
    DBAccessInfo,
)
from spark.utils.spark_session import get_spark_session
from spark.utils.top_list import TOP_SYMBOL_LIST_BY_MARKET_CAP


class SparkAppCryptocurrencyDeltaToMongo(
    SparkBaseAppCryptocurrency
):  # pragma: no cover
    def get_src_path_(self, symbol: str) -> str:
        path_prefix = self.get_path_prefix()
        return f"{path_prefix}/delta/{symbol}"

    def get_dest_path_(self, symbol: str) -> DBAccessInfo:
        return DBAccessInfo(
            uri=f"{MONGO_DRIVER}://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}",
            database=self.MONGO_DATABASE,
            collection=self.MONGO_COLLECTION,
        )

    def read(self, path: str) -> DataFrame:
        return self.spark_session.read.format("delta").load(path)

    def write(self, path: DBAccessInfo, df: DataFrame) -> None:
        return (
            df.write.format("mongo")
            .mode("append")
            .option("mergeSchema", "true")
            .option("uri", path.uri)
            .option("database", path.database)
            .option("collection", path.collection)
            .save()
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
            write_for_this_session(enriched_df.withColumn("symbol", lit(symbol)))


if __name__ == "__main__":  # pragma: no cover
    with get_spark_session("SparkAppCryptocurrencyDeltaToMongo") as spark_session:
        app = SparkAppCryptocurrencyDeltaToMongo(spark_session=spark_session)
        app.run()
