import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Load service account keyfile and create client for BigQuery
keyfile = "dataengineeringcafe-de.json"
# keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "dataengineeringcafe"
dataset_name = "deb_bootcamp"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)



DATA_FOLDER = "data"
data_no_partition = ["addresses", "order_items", "products", "promos"]


def load_data_without_partition(name):
    job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
    file_path = f"{DATA_FOLDER}/{name}.csv"
    df = pd.read_csv(file_path)
    df.info()
    table_id = f"{project_id}.{dataset_name}.{name}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

for data in data_no_partition:
    load_data_without_partition(data)


def load_data_with_partition(name, date, clustering_fields=None, parse_date_fields=[]):
    schema = [bigquery.SchemaField(field, bigquery.SqlTypeNames.TIMESTAMP) for field in parse_date_fields]
    job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=clustering_fields,
        )
    file_path = f"{DATA_FOLDER}/{name}.csv"
    df = pd.read_csv(file_path, parse_dates=parse_date_fields)
    df.info()
    partition = date.replace("-", "")
    table_id = f"{project_id}.{dataset_name}.{name}${partition}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

load_data_with_partition("events", "2021-02-10", parse_date_fields=["created_at"])
load_data_with_partition("orders", "2021-02-10", parse_date_fields=["created_at", "estimated_delivery_at", "delivered_at"])
load_data_with_partition("users", "2020-10-23", clustering_fields=["first_name", "last_name"], parse_date_fields=["created_at", "updated_at"])
