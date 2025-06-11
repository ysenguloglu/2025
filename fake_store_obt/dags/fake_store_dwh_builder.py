from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime as dt
from fake_store_scripts import source_to_stg, stg_to_dwh

with DAG(
    dag_id="fake_store_dwh_build",
    schedule="@daily",
    start_date=dt(2025, 6, 10),
    catchup=False,
    tags=["fake_store"]
    ) as dag:

    # Source to Staging

    source_to_stg_carts_ = PythonOperator(
        task_id="source_to_stg_carts",
        python_callable=source_to_stg.carts_source_to_stg
    )

    source_to_stg_prods_ = PythonOperator(
        task_id="source_to_stg_prods",
        python_callable=source_to_stg.prods_source_to_stg
    )

    source_to_stg_users_ = PythonOperator(
        task_id="source_to_stg_users",
        python_callable=source_to_stg.users_source_to_stg
    )

    # Staging to DWH

    stg_to_dwh_carts_ = PythonOperator(
        task_id="stg_to_dwh_carts",
        python_callable=stg_to_dwh.carts_stg_to_dwh
    )

    stg_to_dwh_prods_ = PythonOperator(
        task_id="stg_to_dwh_prods",
        python_callable=stg_to_dwh.prods_stg_to_dwh
    )

    stg_to_dwh_users_ = PythonOperator(
        task_id="stg_to_dwh_users",
        python_callable=stg_to_dwh.users_stg_to_dwh
    )

    analytics_obt_ = PythonOperator(
        task_id="analytics_obt",
        python_callable=stg_to_dwh.create_obt_dwh
    )
    
    source_to_stg_carts_ >> stg_to_dwh_carts_ >> analytics_obt_
    source_to_stg_prods_ >> stg_to_dwh_prods_ >> analytics_obt_
    source_to_stg_users_ >> stg_to_dwh_users_ >> analytics_obt_