from typing import Final

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.cryptocurrency.price.consume_kafka import (
    CryptocurrencyConsumeKafkaOperator,
)
from utils.pyspark import (
    APPLICATION_PATH_PREFIX,
    DRIVER_MEMORY,
    PACKAGES,
    PY_FILES_PATH,
)

DAG_ID: Final[str] = "cryptocurrency_stream_consumer"

default_args = {
    "owner": "sam",
    "start_date": "2022-03-20T00:00:00Z",
}

NUM_OF_CONSUMER: Final[int] = 5

CRYPTOCURRENCY_CONSUME_TASK_ID_PREFIX: Final[str] = "cryptocurrency_consume_task"

with DAG(
    dag_id=DAG_ID,
    description="cryptocurrency_stream_consumer",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
) as dag:
    cryptocurrency_consume_spark_task = SparkSubmitOperator(
        task_id=f"{CRYPTOCURRENCY_CONSUME_TASK_ID_PREFIX}_s",
        application=f"{APPLICATION_PATH_PREFIX}/cryptocurrency_stream_producer.py",
        packages=PACKAGES,
        py_files=PY_FILES_PATH,
        driver_memory=DRIVER_MEMORY,
    )

    cryptocurrency_consume_python_task = [
        CryptocurrencyConsumeKafkaOperator(
            task_id=f"{CRYPTOCURRENCY_CONSUME_TASK_ID_PREFIX}_{consumer_id}",
        )
        for consumer_id in range(NUM_OF_CONSUMER)
    ]
