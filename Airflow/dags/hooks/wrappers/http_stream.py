from typing import Any

from airflow.providers.http.hooks.http import HttpHook


class HttpStreamHook(HttpHook):
    def __init__(
        self,
        http_conn_id: str,
        method: str = "GET",
        **kwargs,
    ):
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def run(
        self,
        extra_options,
        **kwargs,
    ) -> Any:
        return super().run(
            extra_options={
                "stream": True,
                **extra_options,
            },
            **kwargs,
        )
