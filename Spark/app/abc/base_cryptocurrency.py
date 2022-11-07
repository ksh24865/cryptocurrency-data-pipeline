from abc import ABC
from argparse import ArgumentParser
from typing import Final

from spark.app.abc.base import SparkBaseApp
from spark.constants import BUCKET_NAME


class SparkBaseAppCryptocurrency(SparkBaseApp, ABC):  # pragma: no cover
    MONGO_DATABASE: Final[str] = "cryptocurrency"
    MONGO_COLLECTION: Final[str] = "cryptocurrency"

    def get_path_prefix(self) -> str:
        return f"s3a://{BUCKET_NAME}/crypto_currency"

    def get_arg_parser(self) -> ArgumentParser:
        arg_parser = ArgumentParser()

        return arg_parser
