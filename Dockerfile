FROM apache/airflow:2.7.1-python3.9
USER root

RUN apt update && \
    apt-get clean;

USER airflow

COPY ./requirements.txt /
RUN pip install -r /requirements.txt
