FROM apache/airflow:2.10.2

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt