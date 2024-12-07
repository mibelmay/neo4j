import yaml
import pandas as pd
from minio import Minio
import os
import neo4j
from airflow import AirflowException
from io import StringIO
from datetime import datetime


def get_env_variables() -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Путь к директории скрипта
    config_path = os.path.join(script_dir, "config.yaml")   # Полный путь к config.yaml
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def create_minio_bucket(minio_client: Minio, bucket_name: str) -> None:
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def save_data_to_minio(
    minio_client: Minio, bucket_name: str, object_name: str, data: list
) -> None:
    df = pd.DataFrame(data)
    df.to_csv(object_name, index=False)
    minio_client.fput_object(bucket_name, object_name, object_name)
    os.remove(object_name)


def extract_neo4j_data(driver: neo4j.Driver, query: str) -> list:
    with driver.session() as session:
        result = list(session.run(query))
    return result


def get_requested_files(minio_client: Minio, bucket_name: str, prefix: str) -> list:
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]


def load_data_from_minio(minio_client: Minio, bucket_name: str, object_name: str) -> StringIO:
    if not minio_client.bucket_exists(bucket_name):
        raise AirflowException(f"Bucket with name {bucket_name} is not found")
    try:
        minio_client.stat_object(bucket_name, object_name)
    except minio.error.ResponseError as err:
        if err.code == "NoSuchKey":
            raise AirflowException(
                f"Object with name '{object_name}' is not found in bucket {bucket_name}"
            )
        else:
            raise AirflowException("Problems with minio")
    response = minio_client.get_object(bucket_name, object_name)
    data = response.read().decode("utf-8")
    return StringIO(data)


def set_load_date(tables: list) -> None:
    for df in tables:
        df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
