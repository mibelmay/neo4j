from airflow import DAG
from airflow.utils.dates import days_ago
from minio import Minio
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase
from datetime import date
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

minio_client = Minio(
    "minio:9000", access_key="minio", secret_key="minio123456789", secure=False
)


dag = DAG(
    "extract_neo4j_to_minio",
    default_args=default_args,
    description="extracts data from Neo4j and saves it to Minio",
    schedule_interval=None,
)


def create_minio_bucket(bucket_name: str):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def extract_neo4j_data(uri: str, username: str, password: str, query: str):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        result = list(session.run(query))
    driver.close()
    return result


def extract_and_save_nodes(
    uri: str, username: str, password: str, bucket_name: str, labels: list
):
    for label in labels:
        query = f"MATCH (n:{label}) RETURN n"
        result = extract_neo4j_data(uri, username, password, query)
        data = [record[0] for record in result]
        save_data_to_minio(
            bucket_name, f"{label}-{date.today().strftime("%Y-%m-%d")}.csv", data
        )


def extract_and_save_relationships(
    uri: str,
    username: str,
    password: str,
    bucket_name: str,
    queries: list,
    relationship_types: list,
):
    index = 0
    for query in queries:
        result = extract_neo4j_data(uri, username, password, query)
        data = [record.values() for record in result]
        save_data_to_minio(
            bucket_name,
            f"{relationship_types[index]}-{date.today().strftime("%Y-%m-%d")}.csv",
            data,
        )
        index = index + 1


def extract_and_save_requested_relationships(
    uri: str, username: str, password: str, bucket_name: str, batch_size: int
):
    query = "MATCH (u:User)-[p:REQUESTED]->(r:Request) return u.userId, r.requestId, p.requestDate"
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        result = session.run(query)
        batch = []
        count = 0
        for record in result:
            batch.append(record.values())
            count += 1
            if count % batch_size == 0:
                save_data_to_minio(
                    bucket_name, f"REQUESTED_{count // batch_size}.csv", batch
                )
                batch = []
        if batch:
            save_data_to_minio(
                bucket_name, f"REQUESTED_{count // batch_size + 1}.csv", batch
            )
    driver.close()


def save_data_to_minio(bucket_name: str, object_name: str, data: list):
    df = pd.DataFrame(data)
    df.to_csv(object_name, index=False)
    minio_client.fput_object(bucket_name, object_name, object_name)
    os.remove(object_name)


create_bucket_task = PythonOperator(
    task_id="create_minio_bucket",
    python_callable=create_minio_bucket,
    op_kwargs={
        "bucket_name": date.today().strftime("%Y-%m-%d"),
    },
    dag=dag,
)

extract_nodes_task = PythonOperator(
    task_id="extract_and_save_nodes",
    python_callable=extract_and_save_nodes,
    op_kwargs={
        "uri": "bolt://neo4j:7687",
        "username": "neo4j",
        "password": "123456789",
        "bucket_name": date.today().strftime("%Y-%m-%d"),
        "labels": ["Trend", "Region", "Request", "User"],
    },
    dag=dag,
)


extract_relationships_task = PythonOperator(
    task_id="extract_and_save_relationships",
    python_callable=extract_and_save_relationships,
    op_kwargs={
        "uri": "bolt://neo4j:7687",
        "username": "neo4j",
        "password": "123456789",
        "bucket_name": date.today().strftime("%Y-%m-%d"),
        "queries": [
            "MATCH (u:User)-[:LIVES_IN]->(r:Region) return u.userId, r.regionId",
            "MATCH (t:Trend)-[:CONTAINS]->(r:Request) return t.trendId, r.requestId",
        ],
        "relationship_types": ["LIVES_IN", "CONTAINS"],
    },
    dag=dag,
)


extract_requested_relationships_task = PythonOperator(
    task_id="extract_and_save_requested_relationships",
    python_callable=extract_and_save_requested_relationships,
    op_kwargs={
        "uri": "bolt://neo4j:7687",
        "username": "neo4j",
        "password": "123456789",
        "bucket_name": date.today().strftime("%Y-%m-%d"),
        "batch_size": 50000,
    },
    dag=dag,
)


(
    create_bucket_task
    >> extract_nodes_task
    >> extract_relationships_task
    >> extract_requested_relationships_task
)
