from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import pandas as pd
from utils import *
import json
from collections import defaultdict

config = get_env_variables()

VERTICA_HOST = config["VERTICA_ENDPOINT"]
VERTICA_PORT = config["VERTICA_PORT"]
VERTICA_USER = config["VERTICA_USER"]
VERTICA_PASSWORD = config["VERTICA_PASSWORD"]
VERTICA_DB = config["VERTICA_DB"]

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

vertica_client = VerticaDatabase(
    VERTICA_HOST, VERTICA_PORT, VERTICA_USER, VERTICA_PASSWORD, VERTICA_DB
)

dag = DAG(
    "aggregation",
    default_args=default_args,
    description="aggregate trends",
    schedule_interval=None,
)


def create_aggregate() -> None:
    trends_df = get_trends_aggregate()
    requests_df = get_requests_aggregate()

    result_data = []
    grouped = trends_df.groupby(["trend_id", "region_id"])
    for (trend_id, region_id), group in grouped:
        trend_name = group["trend_name"].iloc[0]
        region = group["region"].iloc[0]
        request_count = group["request_count"].iloc[0]
        min_age = group["min_age"].iloc[0]
        avg_age = group["avg_age"].iloc[0]
        max_age = group["max_age"].iloc[0]

        filtered_requests = requests_df[
            (requests_df["trend_id"] == trend_id)
            & (requests_df["region_id"] == region_id)
        ]

        gender_stats_json = get_gender_stats(filtered_requests)

        result_data.append(
            {
                "trend": trend_name,
                "region": region,
                "request_count": request_count,
                "min_age": min_age,
                "avg_age": avg_age,
                "max_age": max_age,
                "gender_stats": gender_stats_json,
            }
        )

    result_df = pd.DataFrame(result_data)
    quote_column(result_df, 'trend')
    quote_column(result_df, 'region')
    quote_column(result_df, 'gender_stats')
    save_dataframe_to_vertica(vertica_client, result_df, 'data_mart.Trends_Aggregate')


def get_gender_stats(filtered_requests: pd.DataFrame) -> str:
    gender_stats = defaultdict(
        lambda: {"request_count": 0, "popular_keys": defaultdict(int)}
    )

    for _, row in filtered_requests.iterrows():
        gender = "male" if row["gender_id"] == 1 else "female"
        gender_stats[gender]["request_count"] += row["request_count"]
        gender_stats[gender]["popular_keys"][row["request_text"]] += row[
            "request_count"
        ]

    # топ-5
    for gender in gender_stats:
        popular_keys = dict(
            sorted(
                gender_stats[gender]["popular_keys"].items(),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
        )
        gender_stats[gender]["popular_keys"] = popular_keys

    return json.dumps(gender_stats)


def get_trends_aggregate() -> pd.DataFrame:
    trends_data = vertica_client.execute_query(
        "SELECT \
        tn.trend_id, \
        tn.name AS trend_name, \
        COUNT(rq.request_id) AS request_count, \
        MIN(datediff(year, ub.birthdate, now())) AS min_age, \
        ROUND(AVG(datediff(year, ub.birthdate, now())), 0) AS avg_age, \
        MAX(datediff(year, ub.birthdate, now())) AS max_age, \
        l.region_id, \
        rn.name AS region \
    FROM \
        anchor_model.Trend_Name tn \
    JOIN \
        anchor_model.Contains c ON  tn.trend_id = c.trend_id \
    JOIN \
        anchor_model.Requested rq ON c.request_id = rq.request_id \
    JOIN \
        anchor_model.User_Birthdate ub ON rq.user_id = ub.user_id \
    JOIN \
        anchor_model.User_Gender ug ON rq.user_id = ug.user_id \
    JOIN \
        anchor_model.Lives l ON rq.user_id = l.user_id \
    JOIN \
        anchor_model.Region_Name rn ON l.region_id = rn.region_id \
    GROUP BY \
        tn.name, rn.name, tn.trend_id, l.region_id \
    ORDER BY tn.trend_id;"
    )

    return pd.DataFrame(
        trends_data,
        columns=[
            "trend_id",
            "trend_name",
            "request_count",
            "min_age",
            "avg_age",
            "max_age",
            "region_id",
            "region",
        ],
    )


def get_requests_aggregate() -> pd.DataFrame:
    requests_data = vertica_client.execute_query(
        "SELECT \
        rt.text AS request_text, \
        count(*) AS request_count, \
        u.gender_id, \
        l.region_id, \
        c.trend_id \
        FROM anchor_model.Contains c \
        JOIN anchor_model.Request_Text rt ON c.request_id = rt.request_id \
        JOIN anchor_model.Requested r ON c.request_id = r.request_id \
        JOIN anchor_model.User_Gender u ON r.user_id = u.user_id \
        JOIN anchor_model.Lives l ON u.user_id = l.user_id \
        GROUP BY rt.text, u.gender_id, l.region_id, c.trend_id \
        ORDER BY gender_id, l.region_id;"
    )

    return pd.DataFrame(
        requests_data,
        columns=["request_text", "request_count", "gender_id", "region_id", "trend_id"],
    )


aggregate_task = PythonOperator(
    task_id="aggregate_trends",
    python_callable=create_aggregate,
    dag=dag,
)