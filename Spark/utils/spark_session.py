from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession


@contextmanager
def get_spark_session(
    app_name: str = "AnonymousSparkApp",
) -> Iterator[SparkSession]:  # pragma: no cover
    spark_session = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
    spark_session.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key",
        "",
    )

    try:
        yield spark_session
    finally:
        spark_session.stop()
