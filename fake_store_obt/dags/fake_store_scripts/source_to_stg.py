from google.cloud import bigquery
from datetime import datetime as dt
import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
bq_client = bigquery.Client.from_service_account_info(json.loads(os.getenv("G_CLOUD_KEY")))
carts_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.carts"
prods_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.products"
users_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.users"

url_carts = os.getenv('URL_CARTS')
url_prods = os.getenv('URL_PRODS')
url_users = os.getenv('URL_USERS')

def request_get(url_address):
    response = requests.get(url=url_address)
    return response.json()

def get_carts_data():
    data = request_get(url_carts)
    df = pd.json_normalize(
        data,
        meta=["id","userId","date"],
        record_path="products")
    df.rename(columns={
        "date":"cart_date"},
        inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    new_column_order = ["userId","productId","date","quantity"]
    df = df[new_column_order]

    return df

def get_prods_data():
    data = request_get(url_prods)

    df = pd.json_normalize(data)
    df.rename(columns={
        "id":"productId",
        "rating.rate":"rate",
        "rating.count":"comments"},
        inplace=True)

    return df

def get_users_data():
    data = request_get(url_users)
    df = pd.json_normalize(data)
    df.rename(columns={
        "id":"userId",
        "address.geolocation.lat":"latitude",
        "address.geolocation.long":"longitude",
        "address.city":"city",
        "address.street":"street",
        "address.number":"address_no",
        "address.zipcode":"zipcode",
        "name.firstname":"first_name",
        "name.lastname":"last_name"},
        inplace=True)
    
    df = df.drop(["username","password","__v"]
                ,axis=1)

    return df


def carts_source_to_stg():
    df = get_carts_data()
    df["insertion_date"] = dt.today()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    load_job = bq_client.load_table_from_dataframe(
        df, carts_ref, job_config=job_config
    )
    load_job.result()

def prods_source_to_stg():
    df = get_prods_data()
    df["insertion_date"] = dt.today()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    load_job = bq_client.load_table_from_dataframe(
        df, prods_ref, job_config=job_config
    )
    load_job.result()

def users_source_to_stg():
    df = get_users_data()
    df["insertion_date"] = dt.today()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    load_job = bq_client.load_table_from_dataframe(
        df, users_ref, job_config=job_config
    )
    load_job.result()