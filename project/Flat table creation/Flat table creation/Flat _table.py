import data.constants as config_data
from functions.functions import *
from query.flat_table_queries import *
import pandas as pd
import numpy as np
import json
import logging
from datetime import date,datetime, timedelta
import base64
from google.cloud import bigquery

def flat_tables_creation():
    try:
        client = bigquery.Client()
        zero_records_check()
        f=open('data/GA360_master_config_file.json')
        schema=json.load(f)
        e=open('data/table_config_file.json')
        table_columns=json.load(e)
        # Inserting (D-2) day into hits level table
        client.query(generate_query(standard_columns=table_columns["hits_table_columns"],schema=schema,table_name='hit_level_table'))
        logging.info(" Data inserted into hits level table")
        # Inserting (D-2) day into product table
        client.query(generate_query(standard_columns=table_columns["product_table_columns"],schema=schema,table_name='product_table'))
        logging.info(" Data inserted into product table")
        # Inserting (D-2) day into promotion table
        client.query(generate_query(standard_columns=table_columns["promotion_table_columns"],schema=schema,table_name='promotion_table'))
        logging.info(" Data inserted into promotion table")
        success_notification(config_data.notification_name,config_data.report_link)

    except ConnectionError as ce:
        logging.info(f'Connection error occured:{ce}',exc_info=1)
        raise ce     
    except LookupError as le:
        logging.info(f'Key Index error occured:{le}',exc_info=1)
        raise le
    except FileNotFoundError as fe:
        logging.info(f"The file was not found: {fe}",exc_info=1)
        raise fe
    except Exception as e:
        logging.info("Exception has occured",exc_info=1)
        raise e


