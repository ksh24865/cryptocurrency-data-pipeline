from functools import reduce
from typing import Final, List

APPLICATION_PATH_PREFIX: Final[str] = "/opt/airflow/dags/spark/app"
PACKAGES: Final[str] = (
    "com.amazonaws:aws-java-sdk-pom:1.12.31"
    ",org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.2.1"
    ",mysql:mysql-connector-java:8.0.27"
    ",com.crealytics:spark-excel_2.12:0.14.0"
    ",org.xerial:sqlite-jdbc:3.36.0.3"
    ",org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    ",org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
)
PY_FILES_PATH: Final[str] = "/opt/airflow/dags/spark.zip"
DRIVER_MEMORY: Final[str] = "2g"


def kwargs_to_application_args(**kwargs) -> List[str]:
    return reduce(
        lambda x, y: x + y,
        ([f"--{k.replace('_', '-')}", str(v)] for k, v in kwargs.items()),
        [],
    )
