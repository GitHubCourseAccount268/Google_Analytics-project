import base64
import pandas as pd
#import dask.dataframe as pd
#from dask.distributed import Client, LocalCluster
import numpy as np 
import datetime 
import pytz
import requests 
import pandas_gbq
import json 
from pandas import json_normalize 
from google.cloud import bigquery 
from google.cloud import logging
from google.oauth2 import service_account 
# credentials = service_account.Credentials.from_service_account_file('latest.json') 
# client = bigquery.Client(credentials= credentials,project=project_id)
logging_client = logging.Client()
f = '''resource.type="bigquery_dataset" AND resource.labels.dataset_id="analytics_392162047" AND protoPayload.resourceName:"events"'''
for entry in logging_client.list_entries(filter_=f,order_by=logging.DESCENDING):
  if 'events_intraday' not in entry.payload['resourceName'].split('/')[-1]:
    table_name=(entry.payload['resourceName'].split('/')[-1]).split('_')[-1]
client = bigquery.Client()


def missing_data_check(event, context):
  def query_big_query(query):
    
    """
    Query backend return result in the form of dataframe
    :param query: the query to be queried
    """  
    # Cant directly add hence add indirectly
    update_query = client.query(query)
    update_iter = update_query.result()  # Waits for job to complete.
    update_table = update_iter.to_dataframe()
    return

  query_big_query(f'''
  IF EXISTS 
    (SELECT size_bytes FROM `ww-ga-360-big-query.analytics_392162047.__TABLES__` 
    WHERE table_id= 'events_{table_name}')
  THEN 
    INSERT INTO `ww-ga360-bigquery-stellar-bi.MVP_New.ga_alerts_missing_transactions`
    SELECT a.hour_of_the_day,a.minute,a.Country,a.transaction_id,a.revenue
    FROM 
    (SELECT
      concat(event_date,format("%02d",EXTRACT(HOUR FROM datetime(TIMESTAMP_MICROS(event_timestamp))))) as hour_of_the_day,
      EXTRACT(MINUTE FROM datetime(TIMESTAMP_MICROS(event_timestamp))) as minute,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'v1') as Country,
      ecommerce.transaction_id as transaction_id,
      SUM(IFNULL(ecommerce.purchase_revenue_in_usd,0)) as revenue
    FROM `ww-ga-360-big-query.analytics_392162047.events_{table_name}`
    WHERE event_name='purchase'
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3) a LEFT JOIN 
    `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_incremental` b ON
    a.hour_of_the_day=b.hour_of_the_day AND a.minute=b.minute AND a.Country=b.Country 
    AND a.transaction_id=b.transaction_id
  WHERE b.revenue is NULL
  ELSE
  SELECT NULL;
  END IF;
  ''')

  pubsub_message = base64.b64decode(event).decode('utf-8')
  print(pubsub_message)

  print(f"Event_ID: {context.event_id}\n Timestamp: {datetime.datetime.now()}")

  