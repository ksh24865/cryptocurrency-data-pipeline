from datetime import date

from operators.cryptocurrency.price.sourcing_batch import (
    CryptocurrencyPriceSourcingBatchOperator,
)
from utils.date import DAY, get_date_days_before, get_date_years_before, str_to_date


class CryptocurrencyPriceSourcingBatchHourlyOperator(
    CryptocurrencyPriceSourcingBatchOperator
):
    @property
    def batch_unit(self):
        return "hourly"

    @property
    def api_endpoint(self):
        return "histohour"

    @property
    def a_day_per_batch_unit(self):
        return 24

    @property
    def start_date_of_date_range(self) -> date:
        return (
            get_date_years_before(
                current_date=str_to_date(self.execution_date),
                years=self.YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY,
            )
            + DAY
        )

    @property
    def end_date_of_date_range(self) -> date:
        return get_date_days_before(
            current_date=str_to_date(self.execution_date),
            days=self.DAYS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY,
        )
