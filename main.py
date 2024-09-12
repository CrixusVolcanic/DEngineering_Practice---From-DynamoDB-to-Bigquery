import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
from libraries.utils import default_logger, timeit
from libraries import dynamo_class
from libraries.libraries import get_table_dynamodb_v
import os
import pandas as pd
from libraries.bq_utils import execute_query_bigquery
from dotenv import load_dotenv

#load_dotenv()

def fn_total_data_etl():

    dynamoObject = dynamo_class.DynamoClass()

    start_date = datetime.fromisoformat("2019-01-01 00:00:00")

    dynamoObject.fn_extract_table(start_date=start_date, country='CO')

    # crear un df con la data de col
    df = dynamoObject.df_data

    dynamoObject.fn_extract_table(start_date=start_date, country='MX')

    #junta la data de co y mx
    dynamoObject.df_data = pd.concat([df, dynamoObject.df_data])

    dynamoObject.fn_load_to_bq(write_disposition="WRITE_TRUNCATE")

def fn_incremental_data_etl():

    dynamoObject = dynamo_class.DynamoClass()

    query = f"""select country
                    ,min(created_at) min_created_at
                    ,max(created_at) max_created_at
                from `{os.getenv("BQ_PROJECT")}.test.web_global_api_marketing`
                group by 1;"""
    
    df_created_at = execute_query_bigquery(project=os.getenv("BQ_PROJECT"), query=query)

    # Extrae la columna "max_created_at" como una Series
    start_date = df_created_at.loc[df_created_at["country"] == "CO", "max_created_at"]
    # Convierte los datos de la columna a fechas
    start_date = pd.to_datetime(start_date, errors='coerce')
    start_date = start_date.dropna().iloc[0]
    start_date = start_date + pd.Timedelta(milliseconds=1)

    dynamoObject.fn_extract_table(start_date=start_date, country='CO')

    # crear un df con la data de col
    df = dynamoObject.df_data

    # Extrae la columna "max_created_at" como una Series
    start_date = df_created_at.loc[df_created_at["country"] == "MX", "max_created_at"]
    # Convierte los datos de la columna a fechas
    start_date = pd.to_datetime(start_date, errors='coerce')
    start_date = start_date.dropna().iloc[0]
    start_date = start_date + pd.Timedelta(milliseconds=1)

    dynamoObject.fn_extract_table(start_date=start_date, country='MX')

    #concatena la data de co y mx
    dynamoObject.df_data = pd.concat([df, dynamoObject.df_data])

    dynamoObject.fn_load_to_bq(write_disposition="WRITE_APPEND")


def main():
    default_logger.info("Proceso Iniciado")

    #fn_total_data_etl()
    fn_incremental_data_etl()

    default_logger.info(f"Proceso Finalizado")

if __name__ == '__main__':

    main()