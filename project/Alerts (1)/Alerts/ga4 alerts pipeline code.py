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
from google.oauth2 import service_account 
# credentials = service_account.Credentials.from_service_account_file('latest.json') 
# client = bigquery.Client(credentials= credentials,project=project_id)
client = bigquery.Client()


def revenue_check(event, context):
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

  time_diff = 0
  size_bytes=client.query('''SELECT size_bytes FROM `ww-ga-360-big-query.analytics_392162047.__TABLES__` 
    WHERE table_id= 'events_{table_name}''')
  query_big_query(f'''DECLARE LATEST_TIME TIMESTAMP;
  DECLARE T_DIFF INT64 DEFAULT {time_diff};
  DECLARE TIME_HOUR_MINUTE INT64;
  SET TIME_HOUR_MINUTE=CAST(CONCAT(EXTRACT(HOUR FROM datetime(current_timestamp())),EXTRACT(MINUTE FROM datetime(current_timestamp()))) AS INT64);
  IF TIME_HOUR_MINUTE= 0
  THEN
    CREATE TEMP TABLE STAGGED_TABLE AS
    (SELECT * FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_view_d_minus_2`);
    DELETE FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check` WHERE export_id <UNIX_SECONDS(PARSE_TIMESTAMP('%Y-%m-%d',CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS STRING)));
    DELETE FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_incremental` WHERE export_id <UNIX_SECONDS(PARSE_TIMESTAMP('%Y-%m-%d',CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS STRING)));
  ELSE  
    CREATE TEMP TABLE STAGGED_TABLE AS
    (SELECT * FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_view_d_minus_1`);
  END IF;

  SET LATEST_TIME = (SELECT MAX(PARSE_TIMESTAMP('%Y%m%d%H%M',CONCAT(hour_of_the_day,minute))) FROM STAGGED_TABLE);

  INSERT INTO `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check`
  SELECT * FROM STAGGED_TABLE
  WHERE
  PARSE_TIMESTAMP('%Y%m%d%H%M',CONCAT(hour_of_the_day,minute)) < TIMESTAMP_ADD(LATEST_TIME,INTERVAL -T_DIFF MINUTE)
  ''')
  print("Cumulative table populated")

  query_big_query('''
  CREATE TEMP TABLE CHECK_TABLE AS
  (SELECT * EXCEPT (PRIORITY)
  FROM
  (SELECT *,ROW_NUMBER() OVER (PARTITION BY(CONCAT(hour_of_the_day,'_',minute,'_',Country,'_',IFNULL(transaction_id,'NULL'),'_',IFNULL(revenue,0))) ORDER BY export_id) AS PRIORITY 
  FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check`)
  WHERE PRIORITY = 1);

  IF (SELECT COUNT(*) FROM `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_incremental`) < (SELECT COUNT(*) FROM CHECK_TABLE)
  THEN
  CREATE OR REPLACE TABLE `ww-ga360-bigquery-stellar-bi.Example.ga_alerts_store_revenue_check_incremental`
  AS
  (SELECT export_id,hour_of_the_day,minute,Country,transaction_id,abs(revenue) as revenue FROM CHECK_TABLE);
  INSERT INTO `ww-ga360-bigquery-stellar-bi.MVP_New.ga_alerts_negative_revenue_transactions`
  SELECT * FROM CHECK_TABLE
  WHERE revenue <0;
  ELSE
  SELECT NULL;
  END IF;
  ''')
  print("Incremental table populated")

  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  print(pubsub_message)

  print(f"Event_ID: {context.event_id}\n Timestamp: {datetime.datetime.now()}")