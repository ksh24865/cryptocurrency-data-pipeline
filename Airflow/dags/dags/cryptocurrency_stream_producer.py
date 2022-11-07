from typing import Final

from airflow import DAG
from operators.cryptocurrency.price.sourcing_stream import (
    CryptocurrencyPriceSourcingStreamOperator,
)
from utils.cryptocurrency.top_list import top_symbol_list_by_market_cap_generator

DAG_ID: Final[str] = "cryptocurrency_stream_producer"

default_args = {
    "owner": "sam",
    "start_date": "2022-03-20T00:00:00Z",
}

CRYPTOCURRENCY_PRODUCE_TASK_ID_PREFIX: Final[str] = "cryptocurrency_produce_task"


with DAG(
    dag_id=DAG_ID,
    description="cryptocurrency_stream",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
) as dag:

    cryptocurrency_produce_task = [
        CryptocurrencyPriceSourcingStreamOperator(
            task_id=f"{CRYPTOCURRENCY_PRODUCE_TASK_ID_PREFIX}_{idx}",
            symbol_list=top_symbol_list_by_market_cap,
        )
        for idx, top_symbol_list_by_market_cap in enumerate(
            top_symbol_list_by_market_cap_generator()
        )
    ]
