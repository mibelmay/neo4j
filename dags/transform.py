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
VERTICA_HOST = config["VERTICA_ENDPOINT"]
VERTICA_PORT = config["VERTICA_PORT"]
VERTICA_USER = config["VERTICA_USER"]
VERTICA_PASSWORD = config["VERTICA_PASSWORD"]
VERTICA_DB = config["VERTICA_DB"]


BUCKET_NAME = date.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


minio_client = Minio(
    MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)

vertica_client = VerticaDatabase(
    VERTICA_HOST, VERTICA_PORT, VERTICA_USER, VERTICA_PASSWORD, VERTICA_DB
)

dag = DAG(
    "transform",
    default_args=default_args,
    description="load from Minio and transform",
    schedule_interval=None,
)


def transform_regions_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    region_df = df[["regionId"]].rename(columns=renames["region_df"])
    region_name_df = df[["regionId", "name"]].rename(columns=renames["region_name_df"])

    set_load_date([region_df, region_name_df])
    quote_column(region_name_df, "name")

    save_dataframe_to_vertica(vertica_client, region_df, "anchor_model.Region")
    save_dataframe_to_vertica(vertica_client, region_name_df, "anchor_model.Region_Name")
    

def transform_users_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    user_df = df[["userId"]].rename(columns=renames["user_df"])

    user_username_df = df[["userId", "username"]].rename(columns=renames["user_username_df"])
    quote_column(user_username_df, "username")

    user_birthdate_df = df[["userId", "birthdate"]].rename(columns=renames["user_birthdate_df"])
    quote_column(user_birthdate_df, "birthdate")

    user_regdate_df = df[["userId", "regDate"]].rename(columns=renames["user_regdate_df"])
    quote_column(user_regdate_df, "reg_date")

    genders = config["genders"]

    user_gender_df = df[["userId", "gender"]].rename(columns=renames["user_gender_df"])
    user_gender_df["gender_id"] = user_gender_df["gender_id"].map(genders)

    set_load_date(
        [user_df, user_username_df, user_birthdate_df, user_regdate_df, user_gender_df]
    )

    save_dataframe_to_vertica(vertica_client, user_df, "anchor_model.User")
    save_dataframe_to_vertica(vertica_client, user_username_df, "anchor_model.User_Username")
    save_dataframe_to_vertica(vertica_client, user_birthdate_df, "anchor_model.User_Birthdate")
    save_dataframe_to_vertica(vertica_client, user_regdate_df, "anchor_model.User_Regdate")
    save_dataframe_to_vertica(vertica_client, user_gender_df, "anchor_model.User_Gender")


def transform_trends_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    trend_df = df[["trendId"]].rename(columns=renames["trend_df"])

    trend_name_df = df[["trendId", "name"]].rename(columns=renames["trend_name_df"])
    quote_column(trend_name_df, "name")

    statuses = config["statuses"]

    trend_status_df = df[["trendId", "status"]].rename(columns=renames["trend_status_df"])
    trend_status_df["status_id"] = trend_status_df["status_id"].map(statuses)

    set_load_date([trend_df, trend_name_df, trend_status_df])

    save_dataframe_to_vertica(vertica_client, trend_df, "anchor_model.Trend")
    save_dataframe_to_vertica(vertica_client, trend_name_df, "anchor_model.Trend_Name")
    save_dataframe_to_vertica(vertica_client, trend_status_df, "anchor_model.Trend_Status")


def transform_requests_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    request_df = df[["requestId"]].rename(columns=renames["request_df"])

    request_text_df = df[["requestId", "text"]].rename(columns=renames["request_text_df"])
    quote_column(request_text_df, "text")

    request_date_df = df[["requestId", "requestDate"]].rename(columns=renames["request_date_df"])
    quote_column(request_date_df, "date")

    requested_df = df[["userId", "requestId"]].rename(columns=renames["requested_df"])

    set_load_date([request_df, request_text_df, request_date_df, requested_df])

    save_dataframe_to_vertica(vertica_client, request_df, "anchor_model.Request")
    save_dataframe_to_vertica(vertica_client, request_text_df, "anchor_model.Request_Text")
    save_dataframe_to_vertica(vertica_client, request_date_df, "anchor_model.Request_Date")
    save_dataframe_to_vertica(vertica_client, requested_df, "anchor_model.Requested")


def transform_lives_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    lives_df = df[["userId", "regionId"]].rename(columns=renames["lives_df"])
    set_load_date([lives_df])

    save_dataframe_to_vertica(vertica_client, lives_df, "anchor_model.Lives")

    vertica_client.close()


def transform_contains_data(bucket_name: str, object_name: str):
    csv_data = load_data_from_minio(minio_client, bucket_name, object_name)
    df = pd.read_csv(csv_data, index_col=False)

    temp_df = df[["trendId", "text"]].rename(columns=renames["contains_df"])

    contains_df = create_contains_df(vertica_client, temp_df)

    set_load_date([contains_df])

    save_dataframe_to_vertica(vertica_client, contains_df, "anchor_model.Contains")


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
    python_callable=transform_trends_data,
    op_args=[
        BUCKET_NAME,
        f"Trend-{BUCKET_NAME}.csv",
    ],
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

requested_tasks = []
prefix = "REQUESTED_"
requested_files = get_requested_files(minio_client, BUCKET_NAME, prefix)

for i in range(len(requested_files)):
    requested_tasks.append(PythonOperator(
        task_id=f"transform_{requested_files[i]}",
        python_callable=transform_requests_data,
        op_args=[BUCKET_NAME, requested_files[i]],
        dag=dag,
    ))

transform_contains_task = PythonOperator(
    task_id="transform_contains",
    python_callable=transform_contains_data,
    op_args=[
        BUCKET_NAME,
        f"CONTAINS-{BUCKET_NAME}.csv",
    ],
    dag=dag,
)

(
    transform_regions_task
    >> transform_users_task
    >> transform_lives_task
    >> transform_trends_task
    >> requested_tasks
    >> transform_contains_task
)
