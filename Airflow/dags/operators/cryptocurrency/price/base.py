from abc import ABC
from dataclasses import dataclass
from time import sleep
from typing import Any, Dict, Optional, Union

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from constants import CRYPTO_COMPARE_TO_SYMBOL, CRYPTO_COMPARE_V2_HTTP_CONN_ID
from hooks.wrappers.http_stream import HttpStreamHook
from utils.exception import raise_airflow_exception
from utils.proxy import PROXY_IP_LIST, PROXY_PORT
from utils.request import get_request_json


@dataclass
class CryptocurrencyPriceApiData:
    limit: int
    toTs: int
    fsym: str
    aggregate: int = 1
    tsym: str = CRYPTO_COMPARE_TO_SYMBOL


class CryptocurrencyBaseOperator(BaseOperator, ABC):
    cryptocurrency_http_conn_id: str = CRYPTO_COMPARE_V2_HTTP_CONN_ID
    PROXY_IP_IDX: int = 0

    def __init__(
        self,
        api_aggregate: int = 1,
        back_off_base: float = 0,
        back_off_cap: float = 0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.http_hook = HttpStreamHook(
            http_conn_id=self.cryptocurrency_http_conn_id,
        )
        self.api_aggregate = api_aggregate
        self.back_off_base = back_off_base
        self.back_off_cap = back_off_cap

    @property
    def api_endpoint(self) -> str:
        raise NotImplementedError()

    @property
    def api_header(self) -> Dict[str, str]:
        return {}

    @property
    def proxies(self) -> Optional[Dict[str, str]]:
        if self.PROXY_IP_IDX < len(PROXY_IP_LIST):
            proxy_ip = PROXY_IP_LIST[self.PROXY_IP_IDX]
            return {
                "http": f"{proxy_ip}:{PROXY_PORT}",
                "https": f"{proxy_ip}:{PROXY_PORT}",
            }
        else:
            return None

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
        if response_status == "Success":
            return response_json
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

    def write(
        self,
        **kwargs,
    ) -> None:
        raise NotImplementedError()
