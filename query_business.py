import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import pandas as pd
import logging
from libraries.libraries import get_table_dynamodb_v

def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with the given name, log file, and level."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_file)        
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

default_logger = setup_logger('default_logger', 'default.log')

if __name__ == '__main__':

    default_logger.info("Proceso iniciado")

    start_process_now = datetime.now()

    # Calcular la fecha de hace x días
    thirty_days_ago = datetime.now() - timedelta(days=15)

    #df = get_table_dynamodb(start_date=thirty_days_ago, country='CO')

    df =  get_table_dynamodb_v(dynamodb_table='web-global-api-marketing')

    #df.to_json("output-business.json", indent=2, orient="records")

    default_logger.info(f"DataFrame exportado, filas obtenidas {df.shape[0]}")

    finish_process_now = datetime.now()

    default_logger.info(f"Proceo Finalizado, duracio = {finish_process_now - start_process_now}")
