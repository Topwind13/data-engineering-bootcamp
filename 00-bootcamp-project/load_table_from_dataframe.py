# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

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

# Define schema for tables
schema_dict = { "events": [
                    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
                ],
                "orders": [
                    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
                ],
                "users": [
                    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
                ]
}

def get_job_config(file_name):
    # Return job configuration for loading data into BigQuery table based on file_name
    job_config = None
    if file_name in ["events", "orders", "users"]:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema_dict[name],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=None,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        ) 
    return job_config


DATA_FOLDER = "data"
file_name_list = ["events", "orders", "users", "addresses", "order_items", "products", "promos"]
parse_dates_dict = { "events": ["created_at"],
                    "orders": ["created_at", "estimated_delivery_at", "delivered_at"],
                    "users": ["created_at", "updated_at"]}

# Load data from CSV files into BigQuery tables
for name in file_name_list:
    file_path = f"{DATA_FOLDER}/{name}.csv"
    df = pd.read_csv(file_path, parse_dates=parse_dates_dict.get(name))
    df.info()
    # print(df.head())
    table_id = f"{project_id}.{dataset_name}.{name}"
    job = client.load_table_from_dataframe(df, table_id, job_config=get_job_config(name))
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")  # to check finish jobs