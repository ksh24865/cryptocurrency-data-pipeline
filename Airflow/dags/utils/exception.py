from logging import Logger, getLogger

from airflow import AirflowException


def raise_airflow_exception(
    error_msg: str,
    logger: Logger = getLogger(),
):
    logger.exception(error_msg, stack_info=True)
    raise AirflowException(error_msg)
