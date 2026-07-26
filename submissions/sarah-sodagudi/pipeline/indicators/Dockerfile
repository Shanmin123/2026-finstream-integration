# =============================================================
# Standalone Airflow image for the indicators pipeline DAG.
# Author: Sarah Sodagudi
#
# Self-contained on purpose: this submission must run from a clean clone
# (CONTRIBUTING.md) without depending on any other folder in the wider
# FinStreamAI integration tree.
#
# Separate from the repo-root Dockerfile (fundamentals pipeline) rather
# than shared, because this container ALSO needs a real JRE for PySpark
# (the pip package alone only brings the Python bindings + Spark jars,
# not a JVM to run them in) -- keeping it in its own image avoids
# bloating the fundamentals container, which has no Spark dependency.
#
# Base: python:3.8-slim, matching the pinned Python 3.8 requirement.
# Airflow 2.8.1 installed via the official constraints file -- never
# `pip install apache-airflow` bare.
# =============================================================
FROM python:3.8-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN pip install --no-cache-dir "apache-airflow==2.8.1" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

COPY pipeline/indicators/requirements.txt /tmp/pipeline-requirements.txt
RUN pip install --no-cache-dir -r /tmp/pipeline-requirements.txt

WORKDIR /opt/airflow
