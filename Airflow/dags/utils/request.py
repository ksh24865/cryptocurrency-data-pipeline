import json
import time
from contextlib import contextmanager
from io import BytesIO
from json import JSONDecodeError
from random import uniform
from typing import Any, Dict, List, Optional, Union

import requests
from airflow.providers.http.hooks.http import HttpHook
from hooks.wrappers.http_stream import HttpStreamHook
from utils.temporary_file import temporary_file


class ExpoBackoffDecorr:
    def __init__(
        self,
        base: float = 0.11,
        cap: float = 0.51,
    ) -> None:
        self.base = base
        self.cap = cap
        self.sleep = self.base

    def backoff(self) -> float:
        if self.base <= 0:
            return 0
        self.sleep = min(self.cap, uniform(self.base, self.sleep * 3))
        return self.sleep

    def initialize(self) -> float:
        self.sleep = self.base
        return self.sleep


def try_to_get_request_temporary_file(
    http_hook: Union[HttpHook, HttpStreamHook],
    temporary_file_io: BytesIO,
    back_off: ExpoBackoffDecorr,
    endpoint: str,
    retry_count: int = 5,
    err_msg="",
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    proxies: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        with http_hook.run(
            endpoint=endpoint,
            data=data,
            headers=headers,
            extra_options={"proxies": proxies},
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chu   nk_size parameter to None.
                if chunk:
                    temporary_file_io.write(chunk)
            temporary_file_io.seek(0)
            time.sleep(back_off.initialize())
    except requests.exceptions.HTTPError as e:
        time.sleep(back_off.backoff())
        temporary_file_io.truncate(0)
        return try_to_get_request_temporary_file(
            http_hook=http_hook,
            temporary_file_io=temporary_file_io,
            back_off=back_off,
            endpoint=endpoint,
            data=data,
            headers=headers,
            retry_count=retry_count - 1,
            err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {str(e)} \n\n",
            proxies=proxies,
        )
    except requests.RequestException as e:
        time.sleep(back_off.backoff())
        temporary_file_io.truncate(0)
        return try_to_get_request_temporary_file(
            http_hook=http_hook,
            temporary_file_io=temporary_file_io,
            back_off=back_off,
            endpoint=endpoint,
            data=data,
            headers=headers,
            retry_count=retry_count - 1,
            err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {str(e)} \n\n",
            proxies=proxies,
        )
    except JSONDecodeError as e:
        time.sleep(back_off.backoff())
        temporary_file_io.truncate(0)
        return try_to_get_request_temporary_file(
            http_hook=http_hook,
            temporary_file_io=temporary_file_io,
            back_off=back_off,
            endpoint=endpoint,
            data=data,
            headers=headers,
            retry_count=retry_count - 1,
            err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {str(e)} \n\n",
            proxies=proxies,
        )


@contextmanager
def get_request_temporary_file(
    http_hook: Union[HttpHook, HttpStreamHook],
    endpoint: str,
    data: Optional[Dict[str, Any]] = None,
    back_off_base: Optional[float] = None,
    back_off_cap: Optional[float] = None,
    headers: Optional[Dict[str, Any]] = None,
    proxies: Optional[Dict[str, Any]] = None,
):
    with temporary_file() as f:
        try_to_get_request_temporary_file(
            http_hook=http_hook,
            temporary_file_io=f,
            endpoint=endpoint,
            data=data,
            headers=headers,
            back_off=ExpoBackoffDecorr(
                base=back_off_base,
                cap=back_off_cap,
            ),
            proxies=proxies,
        )
        yield f


def get_request_json(
    http_hook: Union[HttpHook, HttpStreamHook],
    endpoint: str,
    data: Optional[Dict[str, Any]] = None,
    back_off_base: Optional[float] = None,
    back_off_cap: Optional[float] = None,
    headers: Optional[Dict[str, Any]] = None,
    proxies: Optional[Dict[str, Any]] = None,
) -> Union[Dict[str, Any], List[Any]]:
    with get_request_temporary_file(
        http_hook=http_hook,
        endpoint=endpoint,
        data=data,
        headers=headers,
        back_off_base=back_off_base,
        back_off_cap=back_off_cap,
        proxies=proxies,
    ) as f:
        response_bytes = f.read()
        response_json = json.loads(response_bytes)
    return response_json
