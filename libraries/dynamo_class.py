from libraries.utils import default_logger, timeit
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import os
from libraries.bq_utils import save_table

class DynamoClass:

    def __init__(self) -> None:
        
        self.df_data = pd.DataFrame()
        self.country = ""

    #@timeit
    def fn_extract_table(self, country = 'CO', start_date=None):

        self.country = country

        default_logger.info(f"\tExtrayendo datos para {country} desde {start_date}")

        # La tabla de business y el índice secundario global
        TABLE = "web-global-api-marketing"
        INDEX_NAME = "country-created_at-index"  # Asegúrate de reemplazar esto con el nombre real de tu índice

        # Crear cliente DynamoDB en AWS
        dynamodb = boto3.resource("dynamodb")
        table_business = dynamodb.Table(TABLE)

        now = datetime.now()
        default_logger.info(f"\tFecha actual: {now}")
        now_iso = now.isoformat()

        try:

            page = 1
            
            filtered_items = []

            # Inicializar una variable para manejar el LastEvaluatedKey
            last_evaluated_key = None

            while True:

                default_logger.info(f"\tPage {page}")

                # Crear los parámetros básicos de la consulta
                query_params = {
                    'IndexName': INDEX_NAME,
                    'KeyConditionExpression': Key("country").eq(country) & Key("created_at").between(start_date.isoformat(), now_iso),
                    'Limit': 50
                }
                
                # Si hay un LastEvaluatedKey, lo incluimos en la consulta
                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key

                # Realizar la consulta usando el índice secundario global
                response = table_business.query(**query_params)

                # Obtener los ítems del resultado
                items = response.get("Items", [])

                # Filtrar los resultados para manejar los registros con 'updated_at' vacío
                for item in items:
                    # Si 'updated_at' está presente y dentro del rango, se considera
                    if "updated_at" in item and item["updated_at"]:
                        updated_at_date = datetime.fromisoformat(item["updated_at"])
                        if start_date <= updated_at_date <= now:
                            filtered_items.append(item)
                    # Si 'updated_at' está vacío, se considera solo 'created_at'
                    else:
                        created_at_date = datetime.fromisoformat(item["created_at"])
                        if start_date <= created_at_date <= now:
                            filtered_items.append(item)

                last_evaluated_key = response.get('LastEvaluatedKey')

                if last_evaluated_key == None:
                    break

                page += 1

            # Convertir los ítems filtrados a un DataFrame de Pandas
            self.df_data = pd.DataFrame(filtered_items)

        except boto3.exceptions.Boto3Error as e:
            default_logger.error(f"Error al realizar la operación de consulta: {e}")

        except Exception as e:
            default_logger.error(f"Otro tipo de error: {e}")

    def fn_load_to_file(self, format = {"CSV", "JSON", "PARQUET"}, path = None):

        if format == 'JSON':
            self.df_data.to_json(path, indent=2, orient="records")

            default_logger.info(f"\tDataFrame exportado, filas obtenidas {self.df_data.shape[0]}")

    def fn_load_to_bq(self, write_disposition):
        """
        Saves the data from the DataFrames to BigQuery tables.
        """

        default_logger.info("\tSaving data on BigQuery")

        try:

            PROJECT = os.getenv("BQ_PROJECT")
            DATASET = 'test'
            TABLE_NAME = f'web_global_api_marketing'

            if not self.df_data.empty:

                save_table(df=self.df_data
                            ,project=PROJECT
                            ,dataset=DATASET
                            ,table_name=TABLE_NAME
                            ,write_disposition=write_disposition)

        except Exception as err:
            default_logger.error(f"\tError saving data: {err}")  # Otros errores