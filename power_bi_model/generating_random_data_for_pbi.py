import requests
import pandas as pd
import os 
from dotenv import load_dotenv
load_dotenv()

def make_post_request(field, count=1000):
    request = requests.post(url=f'https://api.mockaroo.com/api/generate.json?key={os.getenv("MOCKAROO_API_KEY")}&count={count}',
                            json=field)
    response = request.json()
    return pd.json_normalize(response)


def get_mockaroo_data():
    fields_sales = [
        {
            "name":"order_id",
            "type":"Row Number"
        },
        {
            "name":"product_id",
            "type":"Number",
            "min": 1,
            "max": 20,
            "decimals": 0
        },
        {
            "name":"customer_id",
            "type":"Number",
            "min": 1,
            "max": 100,
            "decimals": 0
        },
        {
            "name":"order_date",
            "type":"Datetime",
            "min": "1/1/2023",
            "max": "1/1/2025"
        },
        {
            "name":"quantity",
            "type":"Number",
            "min": 1,
            "max": 10,
            "decimals": 0
        },
        {
            "name":"unit_price",
            "type":"Number",
            "min": 5.0,
            "max": 100.0,
            "decimals": 2
        }
    ]
    df_sales = make_post_request(field=fields_sales)
    df_sales["order_date"] = pd.to_datetime(df_sales["order_date"]).dt.strftime('%d-%m-%Y')

    fields_products = [
        {
            "name":"product_id",
            "type":"Row Number"
        },
        {
            "name":"product_name",
            "type":"Product Name"
        },
        {
            "name":"category",
            "type":"Product Category"
        },
        {
            "name":"unit_cost",
            "type":"Number",
            "min": 1.0,
            "max": 100.0,
            "decimals": 2
        }
    ]
    df_products = make_post_request(field=fields_products, count=20)
    
    fields_customers = [
        {
            "name":"customer_id",
            "type":"Row Number"
        },
        {
            "name":"customer_name",
            "type":"Full Name"
        },
        {
            "name":"city",
            "type":"Custom List",
            "values": ["İstanbul", "Ankara", "İzmir", "Bursa", "Adana", "Gaziantep", "Konya", "Antalya", "Mersin", "Kayseri"]
        }
    ]
    df_customers = make_post_request(field=fields_customers, count=100)

    return df_sales, df_products, df_customers

df_sales, df_products, df_customers = get_mockaroo_data()
df_sales.to_csv('power_bi_model/generated_datasets/mockaroo_sales_data.csv', index=False)
df_products.to_csv('power_bi_model/generated_datasets/mockaroo_products_data.csv', index=False)
df_customers.to_csv('power_bi_model/generated_datasets/mockaroo_customers_data.csv', index=False)