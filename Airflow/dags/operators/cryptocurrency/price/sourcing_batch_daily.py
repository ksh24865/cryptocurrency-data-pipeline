from datetime import date

from constants import CRYPTO_COMPARE_OLDEST_DATE
from operators.cryptocurrency.price.sourcing_batch import (
    CryptocurrencyPriceSourcingBatchOperator,
)
from utils.date import get_date_years_before, str_to_date


class CryptocurrencyPriceSourcingBatchDailyOperator(
    CryptocurrencyPriceSourcingBatchOperator
):
    @property
    def batch_unit(self):
        return "daily"

    @property
    def api_endpoint(self):
        return "histoday"

    @property
    def a_day_per_batch_unit(self):
        return 1

    @property
    def start_date_of_date_range(self) -> date:
        return CRYPTO_COMPARE_OLDEST_DATE

    @property
    def end_date_of_date_range(self) -> date:
        return get_date_years_before(
            current_date=str_to_date(self.execution_date),
            years=self.YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY,
        )
