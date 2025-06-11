from google.cloud import bigquery
from datetime import datetime as dt
import json
import os
from dotenv import load_dotenv

load_dotenv()
bq_client = bigquery.Client.from_service_account_info(json.loads(os.getenv("G_CLOUD_KEY")))
stg_carts_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.carts"
stg_prods_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.products"
stg_users_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_STG")}.users"

dwh_carts_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_DWH")}.carts"
dwh_prods_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_DWH")}.products"
dwh_users_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_DWH")}.users"
dwh_obt_ref = f"{os.getenv("G_CLOUD_PROJECT")}.{os.getenv("G_CLOUD_DATASET_DWH")}.obt_analytics"

def carts_stg_to_dwh():
    query = f"""
        SELECT * FROM {stg_carts_ref}
    """

    job_config=bigquery.QueryJobConfig(
        destination=dwh_carts_ref,
        write_disposition="WRITE_APPEND"
    )

    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()

def prods_stg_to_dwh():
    query = f"""
        SELECT * FROM {stg_prods_ref}
    """

    job_config=bigquery.QueryJobConfig(
        destination=dwh_prods_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()

def users_stg_to_dwh():
    query = f"""
        SELECT * FROM {stg_users_ref}
    """

    job_config=bigquery.QueryJobConfig(
        destination=dwh_users_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()

def create_obt_dwh():
    query = f"""
        select
            c.userId,
            c.productId,
            c.date,
            c.quantity,
            u.latitude,
            u.longitude,
            u.city,
            p.category,
            p.rate,
            p.comments,
            p.price,
            round(c.quantity * p.price,2) as total_amount
        from `data-engineering-101-461108.fake_store_stg.carts` c
        left join `data-engineering-101-461108.fake_store_stg.users` u on c.userId = u.userId
        left join `data-engineering-101-461108.fake_store_stg.products` p on c.productId = p.productId
        order by 1, 3, 2
    """

    job_config=bigquery.QueryJobConfig(
        destination=dwh_obt_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()