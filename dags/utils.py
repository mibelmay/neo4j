import yaml
import pandas as pd
from minio import Minio
import os
import neo4j
from airflow import AirflowException
from io import StringIO
from datetime import datetime
import vertica_python


def get_env_variables() -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    config_path = os.path.join(script_dir, "config.yaml") 
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
        df["load_date"] = f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'"


def quote_column(df: pd.DataFrame, column: str) -> None:
    df[column] = df[column].apply(lambda x: f"'{x}'")


class VerticaDatabase:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.conn_info = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
        }
        self.connection = None
        self.cursor = None
        self.connect() 

    def connect(self) -> None:
        if self.connection is None:
            self.connection = vertica_python.connect(**self.conn_info)
            self.cursor = self.connection.cursor()

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query: str) -> list:
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_row_count(self, table_name: str) -> int:
        query = f"SELECT COUNT(*) FROM {table_name};"
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def fetch_data(self, table_name: str, limit=10) -> list:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        return self.execute_query(query)


def save_dataframe_to_vertica(vertica_client: VerticaDatabase, df: pd.DataFrame, table_name: str) -> None:
    columns = ', '.join(df.columns)
    count = 0
    values = []
    for row in df.values:
        count += 1
        values.append('(' + ', '.join([f"{str(value)}" for value in row]) + ')')
        if count % 5000 == 0:
            query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES {','.join(values)}; COMMIT;
        """
            vertica_client.execute_query(query)
            values = []
    if values:
        query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES {','.join(values)}; COMMIT;
            """
        vertica_client.execute_query(query)


def create_contains_df(vertica_client: VerticaDatabase, df: pd.DataFrame) -> pd.DataFrame:
    data = []

    for index, row in df.iterrows():
        trend_id = row['trend_id']
        text = row['text']
        
        query = f"SELECT request_id FROM anchor_model.Request_Text WHERE text = '{text}'"
        results = vertica_client.execute_query(query)
        
        request_ids = [result[0] for result in results]
        
        for request_id in request_ids:
            data.append({'trend_id': trend_id, 'request_id': request_id})
        
    contains_df = pd.DataFrame(data)
    return contains_df