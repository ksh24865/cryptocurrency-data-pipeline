from dataclasses import asdict, dataclass
from functools import cached_property
from time import sleep
from typing import Any, Dict, List, Optional, Union

from airflow import AirflowException
from airflow.models.taskinstance import Context
from airflow.providers.http.hooks.http import HttpHook
from constants import CRYPTO_COMPARE_HTTP_CONN_ID
from hooks.wrappers.http_stream import HttpStreamHook
from kafka import KafkaProducer
from operators.cryptocurrency.price.base import CryptocurrencyBaseOperator
from utils.exception import raise_airflow_exception
from utils.kafka import kafka_producer_context
from utils.request import get_request_json


@dataclass
class CryptocurrencyMultiPriceApiData:
    fsyms: str
    tsyms: str


class CryptocurrencyPriceSourcingStreamOperator(CryptocurrencyBaseOperator):
    cryptocurrency_http_conn_id: str = CRYPTO_COMPARE_HTTP_CONN_ID

    def __init__(
        self,
        symbol_list: List[str],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.symbol_list = symbol_list

    def read(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ):
        return self.try_to_get_request_json(
            http_hook=self.http_hook,
            endpoint=endpoint,
            data=data,
        )

    @cached_property
    def kafka_topic_name(self) -> str:
        return "cryptocurrency"

    @cached_property
    def standard_currency(self) -> str:
        return "USD"

    @cached_property
    def sleep_second(self) -> float:
        return 1 / len(self.symbol_list)

    @property
    def api_endpoint(self):
        return "pricemulti"

    def try_to_get_request_json(
        self,
        http_hook: Union[HttpHook, HttpStreamHook],
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        retry_count: int = 5,
        err_msg: str = "",
    ) -> Dict[str, Any]:
        if retry_count <= 0:
            raise_airflow_exception(
                error_msg=err_msg,
                logger=self.log,
            )
        try:
            response_json = get_request_json(
                http_hook=http_hook,
                endpoint=endpoint,
                data=data,
                headers=self.api_header,
                back_off_cap=self.back_off_cap,
                back_off_base=self.back_off_base,
                proxies=self.proxies,
            )

        except AirflowException as e:
            self.log.info(f"raise AirflowException err_msg: {e}")
            sleep(10)
            return self.try_to_get_request_json(
                http_hook=http_hook,
                endpoint=endpoint,
                data=data,
                retry_count=retry_count - 1,
                err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {e} \n\n",
            )
        response_status = response_json.get("Response")
        if response_status == "Error":
            response_message = response_json.get("Message")
            if (
                response_message
                == "You are over your rate limit please upgrade your account!"
            ):
                self.PROXY_IP_IDX += 1
                self.log.info(
                    f"{response_message}, raise PROXY_IP_IDX to {self.PROXY_IP_IDX}"
                )
            return self.try_to_get_request_json(
                http_hook=http_hook,
                endpoint=endpoint,
                data=data,
                retry_count=retry_count - 1,
                err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {response_message} \n\n",
            )
        return response_json

    def write(
        self,
        json_data: List[Dict[str, Any]],
        kafka_producer: KafkaProducer,
    ) -> None:
        for data in json_data:
            kafka_producer.send(self.kafka_topic_name, value=data)
            kafka_producer.flush()

    @cached_property
    def api_data(
        self,
    ) -> CryptocurrencyMultiPriceApiData:
        return CryptocurrencyMultiPriceApiData(
            fsyms=",".join(self.symbol_list),
            tsyms=self.standard_currency,
        )

    @staticmethod
    def transform(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {"symbol": symbol, "close": usd.get("USD")} for symbol, usd in data.items()
        ]

    def execute(self, context: Context) -> None:
        # pass
        with kafka_producer_context() as kafka_producer:
            while 1:
                json_data = self.read(
                    endpoint=self.api_endpoint,
                    data=asdict(self.api_data),
                )
                transformed_data = self.transform(data=json_data)
                self.write(
                    json_data=transformed_data,
                    kafka_producer=kafka_producer,
                )
                sleep(10)
