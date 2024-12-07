from airflow import DAG
from airflow.utils.dates import days_ago
from minio import Minio
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import date
from datetime import datetime
from utils import *

config = get_env_variables()

MINIO_ENDPOINT = config["MINIO_ENDPOINT"]
ACCESS_KEY = config["ACCESS_KEY"]
SECRET_KEY = config["SECRET_KEY"]
renames = config["renames"]

BUCKET_NAME = date.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


minio_client = Minio(
    MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)

dag = DAG(
    "transform",
    default_args=default_args,
    description="load from Minio and transform",
    schedule_interval=None,
)


def transform_regions_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    region_df = df[["regionId"]].rename(columns=renames["region_df"])
    region_name_df = df[["regionId", "name"]].rename(columns=renames["region_name_df"])

    set_load_date([region_df, region_name_df])
    # save_data_to_minio(minio_client, "temp", "Region.csv", region_df)
    # save_data_to_minio(minio_client, "temp", "Region_Name.csv", region_name_df)


def transform_users_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    user_df = df[["userId"]].rename(columns=renames["user_df"])

    user_username_df = df[["userId", "username"]].rename(
        columns=renames["user_username_df"]
    )

    user_birthdate_df = df[["userId", "birthdate"]].rename(
        columns=renames["user_birthdate_df"]
    )

    user_regdate_df = df[["userId", "regDate"]].rename(
        columns=renames["user_regdate_df"]
    )

    genders = {"MALE": 1, "FEMALE": 2}

    user_gender_df = df[["userId", "gender"]].rename(columns=renames["user_gender_df"])
    user_gender_df["gender_id"] = user_gender_df["gender_id"].map(genders)

    set_load_date(
        [user_df, user_username_df, user_birthdate_df, user_regdate_df, user_gender_df]
    )
    # save_data_to_minio(minio_client, "temp", "User.csv", user_df)
    # save_data_to_minio(minio_client, "temp", "User_Username.csv", user_username_df)
    # save_data_to_minio(minio_client, "temp", "User_Birthdate.csv", user_birthdate_df)
    # save_data_to_minio(minio_client, "temp", "User_Reg_date.csv", user_regdate_df)
    # save_data_to_minio(minio_client, "temp", "User_Gender.csv", user_gender_df)


def transform_trends_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    trend_df = df[["trendId"]].rename(columns=renames["trend_df"])

    trend_name_df = df[["trendId", "name"]].rename(columns=renames["trend_name_df"])

    statuses = {"active": 1, "inactive": 2}

    trend_status_df = df[["trendId", "status"]].rename(
        columns=renames["trend_status_df"]
    )
    trend_status_df["status_id"] = trend_name_df["status_id"].map(statuses)

    set_load_date([trend_df, trend_name_df, trend_status_df])
    # save_data_to_minio(minio_client, "temp", "Trend.csv", trend_df)
    # save_data_to_minio(minio_client, "temp", "Trend_Name.csv", trend_name_df)
    # save_data_to_minio(minio_client, "temp", "Trend_Status.csv", trend_status_df)


def transform_requests_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    request_df = df[["requestId"]].rename(columns=renames["request_df"])

    request_text_df = df[["requestId", "text"]].rename(
        columns=renames["request_text_df"]
    )

    request_date_df = df[["requestId", "requestDate"]].rename(
        columns=renames["request_date_df"]
    )

    requested_df = df[["userId", "requestId"]].rename(
        columns=renames["requested_df"]
    )

    set_load_date([request_df, request_text_df, request_date_df, requested_df])


def transform_lives_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    lives_df = df[["userId", "regionId"]].rename(
        columns=renames["lives_df"]
    )
    set_load_date([lives_df])


def transform_contains_data(bucket_name: str, object_name: str) -> None:
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    contains_df = df[["trendId", "requestId"]].rename(
        columns=renames["contains_df"]
    )
    set_load_date([contains_df])


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
requested_files = get_requested_files(minio_client, BUCKET_NAME, prefix)

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
