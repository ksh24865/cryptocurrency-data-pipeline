from datetime import datetime
from typing import Iterable, Tuple

from operators.cryptocurrency.price.sourcing_batch import (
    CryptocurrencyPriceSourcingBatchOperator,
)
from utils.date import (
    DAY,
    datetime_range,
    datetime_to_timestamp,
    get_datetime_days_before,
    str_to_date,
    str_to_datetime,
)


class CryptocurrencyPriceSourcingBatchMinutelyOperator(
    CryptocurrencyPriceSourcingBatchOperator
):
    @property
    def batch_unit(self):
        return "minutely"

    @property
    def api_endpoint(self):
        return "histominute"

    @property
    def time_interval(self):
        return self.api_chunk_size * 60

    @property
    def start_date_of_date_range(self) -> datetime:
        return (
            get_datetime_days_before(
                current_date=str_to_date(self.execution_date),
                days=self.DAYS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY,
            )
            + DAY
        )

    @property
    def end_date_of_date_range(self) -> datetime:
        return str_to_datetime(self.execution_date)

    def timestamp_generator(self) -> Iterable[Tuple[int, int]]:
        for start_datetime, end_datetime in datetime_range(
            start_datetime=self.start_date_of_date_range,
            end_datetime=self.end_date_of_date_range,
            time_interval=self.time_interval,
        ):
            print(f"start_datetime ~ end_datetime: {start_datetime} ~ {end_datetime}")
            yield datetime_to_timestamp(start_datetime), self.api_chunk_size
