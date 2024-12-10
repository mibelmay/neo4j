from airflow import DAG
from airflow.utils.dates import days_ago
from minio import Minio
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase
from datetime import date
from utils import *

config = get_env_variables()

MINIO_ENDPOINT = config["MINIO_ENDPOINT"]
ACCESS_KEY = config["ACCESS_KEY"]
SECRET_KEY = config["SECRET_KEY"]
NEO4J_ENDPOINT = config["NEO4J_ENDPOINT"]
NEO4J_USERNAME = config["NEO4J_USERNAME"]
NEO4J_PASSWORD = config["NEO4J_PASSWORD"]

BATCH_SIZE = config["BATCH_SIZE"]
BUCKET_NAME = date.today().strftime("%Y-%m-%d")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

neo4j_driver = GraphDatabase.driver(
    NEO4J_ENDPOINT, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

minio_client = Minio(
    MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)


dag = DAG(
    "extract_neo4j_to_minio",
    default_args=default_args,
    description="extracts data from Neo4j and saves it to Minio",
    schedule_interval=None,
)


def extract_and_save_nodes(bucket_name: str, labels: list) -> None:
    for label in labels:
        query = f"MATCH (n:{label}) RETURN n"
        result = extract_neo4j_data(neo4j_driver, query)
        data = [record[0] for record in result]
        save_data_to_minio(
            minio_client, bucket_name, f"{label}-{BUCKET_NAME}.csv", data
        )


def extract_and_save_relationships(
    bucket_name: str,
    queries: list,
    relationship_types: list,
    columns: list
) -> None:
    index = 0
    for query in queries:
        result = extract_neo4j_data(neo4j_driver, query)
        data = pd.DataFrame(result, columns=columns[index])
        save_data_to_minio(
            minio_client,
            bucket_name,
            f"{relationship_types[index]}-{BUCKET_NAME}.csv",
            data,
        )
        index = index + 1


def extract_and_save_requested_relationships(bucket_name: str, batch_size: int, columns: list) -> None:
    query = "MATCH (u:User)-[p:REQUESTED]->(r:Request) return u.userId, r.requestId, r.text, p.requestDate"
    with neo4j_driver.session() as session:
        result = session.run(query)
        batch = []
        count = 0
        for record in result:
            batch.append(record.values())
            count += 1
            if count % batch_size == 0:
                data = pd.DataFrame(batch, columns=columns)
                save_data_to_minio(
                    minio_client,
                    bucket_name,
                    f"REQUESTED_{count // batch_size}.csv",
                    data,
                )
                batch = []
        if batch:
            data = pd.DataFrame(batch, columns=columns)
            save_data_to_minio(
                minio_client,
                bucket_name,
                f"REQUESTED_{count // batch_size + 1}.csv",
                data,
            )


create_bucket_task = PythonOperator(
    task_id="create_minio_bucket",
    python_callable=create_minio_bucket,
    op_kwargs={
        "minio_client": minio_client,
        "bucket_name": BUCKET_NAME,
    },
    dag=dag,
)

extract_nodes_task = PythonOperator(
    task_id="extract_and_save_nodes",
    python_callable=extract_and_save_nodes,
    op_kwargs={
        "bucket_name": BUCKET_NAME,
        "labels": ["Trend", "Region", "User"],
    },
    dag=dag,
)


extract_relationships_task = PythonOperator(
    task_id="extract_and_save_relationships",
    python_callable=extract_and_save_relationships,
    op_kwargs={
        "bucket_name": BUCKET_NAME,
        "queries": [
            "MATCH (u:User)-[:LIVES_IN]->(r:Region) return u.userId, r.regionId",
            "MATCH (t:Trend)-[:CONTAINS]->(r:Request) return t.trendId, r.requestId",
        ],
        "relationship_types": ["LIVES_IN", "CONTAINS"],
        "columns": [
            ["userId", "regionId"],
            ["trendId", "requestId"]
        ]
    },
    dag=dag,
)


extract_requested_relationships_task = PythonOperator(
    task_id="extract_and_save_requested_relationships",
    python_callable=extract_and_save_requested_relationships,
    op_kwargs={
        "bucket_name": BUCKET_NAME,
        "batch_size": BATCH_SIZE,
        "columns": ["userId", "requestId", "text", "requestDate"]
    },
    dag=dag,
)


(
    create_bucket_task
    >> extract_nodes_task
    >> extract_relationships_task
    >> extract_requested_relationships_task
)
