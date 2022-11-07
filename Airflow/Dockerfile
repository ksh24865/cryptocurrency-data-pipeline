FROM apache/airflow:2.2.0-python3.8
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN pip install kafka-python
RUN pip install apache-airflow-providers-mongo
USER root
RUN apt-get update
RUN apt-get install default-jdk -y

