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

BUCKET_NAME = date.today().strftime("%Y-%m-%d")

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


def transform_trends_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    trend_df = df[["trendId"]].rename(columns={"trendId": "trend_id"})
    trend_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    trend_name_df = df[["trendId", "name"]].rename(columns={"trendId": "trend_id"})
    trend_name_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    statuses = {"active": 1, "inactive": 2}
    trend_status_df = df[["trendId", "status"]].rename(
        columns={"trendId": "trend_id", "status": "status_id"}
    )
    trend_status_df["status_id"] = trend_name_df["status_id"].map(statuses)
    trend_status_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # save_data_to_minio("temp", "Trend.csv", trend_df)
    # save_data_to_minio("temp", "Trend_Name.csv", trend_name_df)
    # save_data_to_minio("temp", "Trend_Status.csv", trend_status_df)


def get_requested_files(bucket_name, prefix):
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]


def transform_requests_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    request_df = df[["requestId"]].rename(columns={"requestId": "request_id"})
    request_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    request_text_df = df[["requestId", "text"]].rename(
        columns={"requestId": "request_id"}
    )
    request_text_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    request_date_df = df[["requestId", "requestDate"]].rename(
        columns={"requestId": "request_id", "requestDate": "date"}
    )
    request_date_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    requested_df = df[["userId", "requestId"]].rename(
        columns={"userId": "user_id", "requestId": "request_id"}
    )
    requested_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def transform_lives_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    lives_df = df[["userId", "regionId"]].rename(
        columns={"userId": "user_id", "regionId": "region_id"}
    )
    lives_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def transform_contains_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    contains_df = df[["trendId", "requestId"]].rename(
        columns={"trendId": "trend_id", "requestId": "request_id"}
    )
    contains_df["load_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_data_to_minio(bucket_name: str, object_name: str, df: list):
    df.to_csv(object_name, index=False)
    minio_client.fput_object(bucket_name, object_name, object_name)
    os.remove(object_name)


transform_regions_task = PythonOperator(
    task_id="transform_regions",
    python_callable=transform_regions_data,
    op_args=[
        BUCKET_NAME,
        f"Region-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)

transform_users_task = PythonOperator(
    task_id="transform_users",
    python_callable=transform_users_data,
    op_args=[
        BUCKET_NAME,
        f"User-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)

transform_trends_task = PythonOperator(
    task_id="transform_trends",
    python_callable=transform_requests_data,
    op_args=[
        BUCKET_NAME,
        f"Trend-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)


prefix = "REQUESTED_"
requested_files = get_requested_files(BUCKET_NAME, prefix)

for file_name in requested_files:
    transform_requests_task = PythonOperator(
        task_id=f"transform_{file_name}",
        python_callable=transform_requests_data,
        op_args=[BUCKET_NAME, file_name],
        dag=dag,
    )

transform_lives_task = PythonOperator(
    task_id="transform_lives",
    python_callable=transform_lives_data,
    op_args=[
        BUCKET_NAME,
        f"LIVES_IN-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)


transform_contains_task = PythonOperator(
    task_id="transform_contains",
    python_callable=transform_contains_data,
    op_args=[
        BUCKET_NAME,
        f"CONTAINS-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)
