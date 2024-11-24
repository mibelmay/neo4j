from airflow import DAG
from airflow.utils.dates import days_ago
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow import AirflowException
from io import StringIO
import pandas as pd
import os
from datetime import date
from datetime import datetime

ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
ENDPOINT_URL = "minio:9000"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


minio_client = Minio(
    ENDPOINT_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)

dag = DAG(
    "transform",
    default_args=default_args,
    description="load from Minio and transform",
    schedule_interval=None,
)


def load_data_from_minio(bucket_name: str, object_name: str):
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


def transform_regions_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    region_df = df[["regionId"]].rename(columns={"regionId": "region_id"})
    region_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    region_name_df = df[["regionId", "name"]].rename(columns={"regionId": "region_id"})
    region_name_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # save_data_to_minio("temp", "Region.csv", region_df)
    # save_data_to_minio("temp", "Region_Name.csv", region_name_df)


def transform_users_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    user_df = df[["userId"]].rename(columns={"userId": "user_id"})
    user_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    user_username_df = df[["userId", "username"]].rename(columns={"userId": "user_id"})
    user_username_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    user_birthdate_df = df[["userId", "birthdate"]].rename(
        columns={"userId": "user_id"}
    )
    user_birthdate_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    user_regdate_df = df[["userId", "regDate"]].rename(
        columns={"userId": "user_id", "regDate": "reg_date"}
    )
    user_regdate_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    genders = {"MALE": 1, "FEMALE": 2}

    user_gender_df = df[["userId", "gender"]].rename(
        columns={"userId": "user_id", "gender": "gender_id"}
    )
    user_gender_df["gender_id"] = user_gender_df["gender_id"].map(genders)
    user_gender_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # save_data_to_minio("temp", "User.csv", user_df)
    # save_data_to_minio("temp", "User_Username.csv", user_username_df)
    # save_data_to_minio("temp", "User_Birthdate.csv", user_birthdate_df)
    # save_data_to_minio("temp", "User_Reg_date.csv", user_regdate_df)
    # save_data_to_minio("temp", "User_Gender.csv", user_gender_df)


def save_data_to_minio(bucket_name: str, object_name: str, df: list):
    df.to_csv(object_name, index=False)
    minio_client.fput_object(bucket_name, object_name, object_name)
    os.remove(object_name)


transform_regions_task = PythonOperator(
    task_id="transform_regions",
    python_callable=transform_regions_data,
    op_args=[
        date.today().strftime("%Y-%m-%d"),
        f'Region-{date.today().strftime("%Y-%m-%d")}.csv',
    ],
    dag=dag,
)

transform_users_task = PythonOperator(
    task_id="transform_users",
    python_callable=transform_users_data,
    op_args=[
        date.today().strftime("%Y-%m-%d"),
        f'User-{date.today().strftime("%Y-%m-%d")}.csv',
    ],
    dag=dag,
)
