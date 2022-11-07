from functools import partial

from pyspark.sql import DataFrame

from spark.app.abc.base_cryptocurrency import SparkBaseAppCryptocurrency
from spark.transformation_factory.cryptocurrency_stream import (
    TransformationFactoryCryptocurrencyStream,
)
from spark.utils.data_frame import divide_by_rows
from spark.utils.db_access_info import (
    MONGO_DRIVER,
    MONGO_HOST,
    MONGO_PASSWORD,
    MONGO_PORT,
    MONGO_USER,
    DBAccessInfo,
)
from spark.utils.spark_session import get_spark_session

TRANSFORMATION = TransformationFactoryCryptocurrencyStream().build()  # pragma: no cover


class SparkAppCryptocurrencyStream(SparkBaseAppCryptocurrency):  # pragma: no cover
    transformation = TRANSFORMATION
    kafka_bootstrap_servers = "kafka:9092"
    topic = "cryptocurrency"

    def get_dest_path_(self) -> DBAccessInfo:
        return DBAccessInfo(
            uri=f"{MONGO_DRIVER}://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}",
            database=self.MONGO_DATABASE,
            collection=self.MONGO_COLLECTION,
        )

    def read_(self) -> DataFrame:
        return (
            self.spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            # .option("startingOffsets", "earliest")
            .load()
        )

    def write(self, path: DBAccessInfo, df: DataFrame) -> None:
        def write_mongo_row(df, _):
            df_tuple = divide_by_rows(df)
            for now_df in df_tuple:

                symbol = now_df.first()["symbol"]
                (
                    df.write.format("mongo")
                    .mode("append")
                    .option("uri", path.uri)
                    .option("database", path.database)
                    .option("collection", symbol)
                    .save()
                )
            pass

        query = df.writeStream.foreachBatch(write_mongo_row).start()
        query.awaitTermination()

    def run(self) -> None:
        arg_parser = self.get_arg_parser()
        args = arg_parser.parse_args()
        dest_path = self.get_dest_path_()
        pure_enrichment = self.purify_enrichment(args)

        read_for_this_session = self.read_
        write_for_this_session = partial(self.write, dest_path)
        enrich_for_this_session = partial(self.enrich, pure_enrichment)

        src_df = read_for_this_session()
        transformed_df = self.transform(src_df)
        enriched_df = enrich_for_this_session(transformed_df)
        write_for_this_session(enriched_df)


if __name__ == "__main__":  # pragma: no cover
    with get_spark_session("SparkAppCryptocurrencyStream") as spark_session:
        spark_app_cryptocurrency_stream = SparkAppCryptocurrencyStream(
            spark_session=spark_session,
        )
        spark_app_cryptocurrency_stream.run()
