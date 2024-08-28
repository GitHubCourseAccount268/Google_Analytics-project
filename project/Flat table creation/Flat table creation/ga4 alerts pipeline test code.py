import base64
import pandas as pd
#import dask.dataframe as pd
#from dask.distributed import Client, LocalCluster
import numpy as np 
import datetime 
import time
import pytz
import requests 
import pandas_gbq
import json 
from pandas import json_normalize 
from google.cloud import bigquery 
from google.oauth2 import service_account 
# credentials = service_account.Credentials.from_service_account_file('latest.json') 
# client = bigquery.Client(credentials= credentials,project=project_id)
time.sleep(60)
client = bigquery.Client()

def revenue_check_test(event, context):
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
  if (datetime.datetime.now().minute-30)<0:
    time_diff_test=datetime.datetime.now().minute-0
  else:
    time_diff_test=datetime.datetime.now().minute-30

  query_big_query(f'''DECLARE LATEST_TIME_TEST TIMESTAMP;
  DECLARE T_DIFF_TEST INT64 DEFAULT {time_diff_test};
  DECLARE TIME_HOUR_TEST INT64;
  SET TIME_HOUR_TEST=CAST(EXTRACT(HOUR FROM datetime(current_timestamp())) AS INT64);
  IF TIME_HOUR_TEST= 0
  THEN
    CREATE TEMP TABLE STAGGED_TEST_TABLE AS
    (SELECT * FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_view_d_minus_2`);
  ELSE  
    CREATE TEMP TABLE STAGGED_TEST_TABLE AS
    (SELECT * FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_view_d_minus_1`);
  END IF;

  SET LATEST_TIME_TEST = (SELECT MAX(PARSE_TIMESTAMP('%Y%m%d%H%M',CONCAT(hour_of_the_day,minute))) FROM STAGGED_TEST_TABLE);

  INSERT INTO `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_test_without_check`
  SELECT * FROM STAGGED_TEST_TABLE;
  INSERT INTO `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_test`
  SELECT * FROM STAGGED_TEST_TABLE
  WHERE
  PARSE_TIMESTAMP('%Y%m%d%H%M',CONCAT(hour_of_the_day,minute)) < TIMESTAMP_ADD(LATEST_TIME_TEST,INTERVAL -T_DIFF_TEST MINUTE)
  ''')
  print("Cumulative table populated")

  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  print(pubsub_message)

  print(f"Event_ID: {context.event_id}\n Timestamp: {datetime.datetime.now()}")


#resource.type="cloud_function" AND resource.labels.function_name="ge_alerts_test_function" AND severity>=ERROR OR "finished with status: 'crash'" OR "finished with status: 'timeout'" OR "finished with status: 'error'" OR "finished with status: 'out of memory'" OR "finished with status: 'out of quota'" OR "finished with status: 'load error'" OR "finished with status: 'load timeout'" OR "finished with status: 'connection error'" OR "finished with status: 'invalid header'" OR "finished with status: 'request too large'" OR "finished with status: 'response error'" OR "finished with status: 'system error" OR "finished with status: 'invalid message'"
