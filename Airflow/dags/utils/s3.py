import json
from io import BytesIO
from typing import Any, Dict, List, Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_s3(
    s3_hook: S3Hook,
    bucket_name: str,
    key: str,
    file_obj: BytesIO,
    replace: bool = True,
) -> None:
    s3_hook.load_file_obj(
        file_obj=file_obj,
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )


def upload_json_to_s3(
    s3_hook: S3Hook,
    bucket_name: str,
    data_key: str,
    json_data: Union[Dict[str, Any], List[Any]],
    replace: bool = True,
) -> None:
    s3_hook.load_string(
        string_data=json.dumps(json_data, ensure_ascii=False),
        key=data_key,
        bucket_name=bucket_name,
        replace=replace,
    )


def upload_bytes_to_s3(
    s3_hook: S3Hook,
    bucket_name: str,
    data_key: str,
    bytes_data: bytes,
    replace: bool = True,
) -> None:
    s3_hook.load_bytes(
        bytes_data=bytes_data,
        key=data_key,
        bucket_name=bucket_name,
        replace=replace,
    )
