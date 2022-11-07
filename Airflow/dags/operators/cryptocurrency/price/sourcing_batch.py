from dataclasses import asdict
from datetime import date
from typing import Any, Dict, Final, Iterable, Tuple

from airflow.models.taskinstance import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.cryptocurrency.price.base import (
    CryptocurrencyBaseOperator,
    CryptocurrencyPriceApiData,
)
from utils.cryptocurrency.top_list import TOP_SYMBOL_LIST_BY_MARKET_CAP
from utils.date import date_range, datetime_to_timestamp
from utils.s3 import upload_json_to_s3


class CryptocurrencyPriceSourcingBatchOperator(CryptocurrencyBaseOperator):
    YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY: Final[int] = 7
    DAYS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY: Final[int] = 7
    template_fields = ("execution_date",)

    def __init__(
        self,
        bucket_name: str,
        execution_date: str,
        api_chunk_size: int = 2000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.execution_date = execution_date
        self.s3_hook = S3Hook()
        self.api_chunk_size = api_chunk_size

    @property
    def batch_unit(self):
        raise NotImplementedError()

    @property
    def a_day_per_batch_unit(self):
        raise NotImplementedError()

    @property
    def time_interval(self):
        return self.api_chunk_size // self.a_day_per_batch_unit

    @property
    def start_date_of_date_range(self) -> date:
        raise NotImplementedError()

    @property
    def end_date_of_date_range(self) -> date:
        raise NotImplementedError()

    def write(
        self,
        json_data: Dict[str, Any],
        key: str,
    ) -> None:
        upload_json_to_s3(
            s3_hook=self.s3_hook,
            bucket_name=self.bucket_name,
            data_key=key,
            json_data=json_data,
        )

    def timestamp_generator(self) -> Iterable[Tuple[int, int]]:
        for start_date, end_date in date_range(
            start_date=self.start_date_of_date_range,
            end_date=self.end_date_of_date_range,
            time_interval=self.time_interval,
        ):
            print(f"start_date ~ end_date: {start_date} ~ {end_date}")
            to_ts = datetime_to_timestamp(end_date)
            days_interval = (end_date - start_date).days
            yield to_ts, days_interval * self.a_day_per_batch_unit

    def api_data_generator(
        self,
        symbol: str,
    ) -> Iterable[CryptocurrencyPriceApiData]:
        for timestamp, limit in self.timestamp_generator():
            yield CryptocurrencyPriceApiData(
                fsym=symbol,
                aggregate=self.api_aggregate,
                limit=limit,
                toTs=timestamp,
            )

    def execute(self, context: Context) -> None:
        data_idx = 0
        for symbol in TOP_SYMBOL_LIST_BY_MARKET_CAP:
            data_key_prefix = f"crypto_currency/json/{symbol}/{self.batch_unit}"
            for data in self.api_data_generator(symbol=symbol):
                json_data = self.read(
                    endpoint=self.api_endpoint,
                    data=asdict(data),
                )
                self.write(
                    json_data=json_data, key=f"{data_key_prefix}/{data_idx}.json"
                )
                data_idx += 1
