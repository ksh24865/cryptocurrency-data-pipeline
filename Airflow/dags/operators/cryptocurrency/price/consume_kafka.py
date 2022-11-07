from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List, Tuple, Union

from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from kafka.consumer.fetcher import ConsumerRecord
from pymongo import MongoClient
from utils.kafka import kafka_consumer_context
from utils.mongo import MONGO_HOST, MONGO_PASSWORD, MONGO_PORT, MONGO_USER


class CryptocurrencyConsumeKafkaOperator(BaseOperator):
    @staticmethod
    def read(
        consumed_data: ConsumerRecord,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        return consumed_data.value

    @cached_property
    def mongo_client(self) -> MongoClient:
        return MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USER,
            password=MONGO_PASSWORD,
        )

    @staticmethod
    def transform(
        data: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Tuple[Union[Dict[str, Any], List[Dict[str, Any]]], str]:
        symbol = data.pop("symbol")
        return {
            **data,
            **{"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        }, symbol

    def write(self, data, symbol) -> None:
        self.mongo_client.cryptocurrency[symbol].insert_one(document=data)

    def execute(self, context: Context) -> None:
        with kafka_consumer_context(topic_name="cryptocurrency") as kafka_consumer:
            for consumed_data in kafka_consumer:
                data_raw = self.read(consumed_data)
                data_transformed, symbol = self.transform(data_raw)
                self.write(
                    data=data_transformed,
                    symbol=symbol,
                )
