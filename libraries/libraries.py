"""libraries to extract data for dynamo table"""

import logging
import boto3
from dynamodb_json import json_util as json
import pandas as pd
import time
from functools import wraps
import os


logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s: %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.info(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')

        return result

    return timeit_wrapper


@timeit
def get_table_dynamodb_v(dynamodb_table):

    client_dydb = boto3.client('dynamodb')
    items = []

    scan_kwargs = {
            'TableName': dynamodb_table,
            'Limit': 50
    }

    n_request = 0
    n_items = 0

    try:

        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = client_dydb.scan(**scan_kwargs)
            n_request += 1
            n_items += len(response.get('Items', []))
            logger.info(f"Number of requests: {n_request}, Number of total items: {n_items}")
            items.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None


    except Exception as err:
        logger.error(f"Couldn't scan. Here's why: {err}")
        raise

    df = pd.DataFrame(json.loads(items))
    df = df.astype('string')
    # create_at_1 = pd.to_datetime(df['created_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
    # created_at_2 = pd.to_datetime(df['created_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    # df['created_at'] = create_at_1.fillna(created_at_2)

    # updated_at_1 = pd.to_datetime(df['updated_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
    # updated_at_2 = pd.to_datetime(df['updated_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    # df['updated_at'] = updated_at_1.fillna(updated_at_2)

    return df
