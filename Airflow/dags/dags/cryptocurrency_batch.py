from typing import Final, Optional, TypedDict

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from constants import CRYPTOCURRENCY_BUCKET_NAME
from operators.cryptocurrency.price.sourcing_batch_daily import (
    CryptocurrencyPriceSourcingBatchDailyOperator,
)
from operators.cryptocurrency.price.sourcing_batch_hourly import (
    CryptocurrencyPriceSourcingBatchHourlyOperator,
)
from operators.cryptocurrency.price.sourcing_batch_minutely import (
    CryptocurrencyPriceSourcingBatchMinutelyOperator,
)
from utils.date import utc_to_kst
from utils.pyspark import (
    APPLICATION_PATH_PREFIX,
    DRIVER_MEMORY,
    PACKAGES,
    PY_FILES_PATH,
)

DAG_ID: Final[str] = "cryptocurrency_batch"

default_args = {
    "owner": "sam",
    "start_date": "2022-03-20T00:00:00Z",
}

SOURCING_DAILY_TASK_ID: Final[str] = "sourcing_daily"
SOURCING_HOURLY_TASK_ID: Final[str] = "sourcing_hourly"
SOURCING_MINUTELY_TASK_ID: Final[str] = "sourcing_minutely"
JSON_TO_DELTA_TASK_ID: Final[str] = "json_to_delta"
JSON_TO_DELTA_DAILY_TASK_ID: Final[str] = "json_to_delta_daily"
JSON_TO_DELTA_HOURLY_TASK_ID: Final[str] = "json_to_delta_hourly"
JSON_TO_DELTA_MINUTELY_TASK_ID: Final[str] = "json_to_delta_minutely"
DELTA_TO_MONGO_MINUTELY_TASK_ID: Final[str] = "delta_to_mongo"


class BatchInfo(TypedDict):
    batch_type: str
    start_date: Optional[str]
    current_date: Optional[str]


class DagRunConf(TypedDict):
    account_id: Optional[str]
    account_name: Optional[str]
    data_category: str
    sourcing_method: str
    provider: str
    user_id: Optional[int]
    batch_info: BatchInfo


CRYPTOCURRENCY_PRICE_SOURCING_BATCH_TASK_ID_PREFIX = (
    "cryptocurrency_price_sourcing_batch_task"
)


with DAG(
    dag_id=DAG_ID,
    description="cryptocurrency_batch",
    default_args=default_args,
    schedule_interval=None,
    user_defined_macros={
        "utc_to_kst": utc_to_kst,
    },
    render_template_as_native_obj=True,
) as dag:

    sourcing_daily = CryptocurrencyPriceSourcingBatchDailyOperator(
        task_id=SOURCING_DAILY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    sourcing_hourly = CryptocurrencyPriceSourcingBatchHourlyOperator(
        task_id=SOURCING_HOURLY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    sourcing_minutely = CryptocurrencyPriceSourcingBatchMinutelyOperator(
        task_id=SOURCING_MINUTELY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    json_to_delta = SparkSubmitOperator(
        task_id=JSON_TO_DELTA_TASK_ID,
        application=f"{APPLICATION_PATH_PREFIX}/cryptocurrency_json_to_delta.py",
        packages=PACKAGES,
        py_files=PY_FILES_PATH,
        driver_memory=DRIVER_MEMORY,
    )

    delta_to_mongo = SparkSubmitOperator(
        task_id=DELTA_TO_MONGO_MINUTELY_TASK_ID,
        application=f"{APPLICATION_PATH_PREFIX}/cryptocurrency_delta_to_mongo.py",
        packages=PACKAGES,
        py_files=PY_FILES_PATH,
        driver_memory=DRIVER_MEMORY,
    )

    (
        [sourcing_daily, sourcing_hourly, sourcing_minutely]
        >> json_to_delta
        >> delta_to_mongo
    )
