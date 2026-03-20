FROM apache/airflow:2.8.1

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    great_expectations==0.18.12 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    dbt-core==1.7.0 \
    dbt-spark==1.7.0 \
    prometheus-client==0.19.0 \
    slack-sdk==3.26.0 \
    yfinance==0.2.36