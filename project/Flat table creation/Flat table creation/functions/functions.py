import logging
import pandas as pd
import numpy as np
import json
import pymsteams
import data.constants as config_data
from query.flat_table_queries import *
from datetime import date,datetime, timedelta
from google.cloud import bigquery

client = bigquery.Client()
def query_big_query(query):
    
    update_query = client.query(query)
    update_iter = update_query.result()
    update_table = update_iter.to_dataframe()
    return update_table

def zero_records_check():
    try:
        # Checking the count of raw ga session table for (D-2) day for apj region
        apj_count = query_big_query(records_check_query.format(config_data.biquery_project_name,config_data.bigquery_apj_propertyid))
        # Checking the count of raw ga session table for (D-2) day for lam region
        lam_count = query_big_query(records_check_query.format(config_data.biquery_project_name,config_data.bigquery_lam_propertyid))
        # Checking the count of raw ga session table for (D-2) day for emea region
        emea_count = query_big_query(records_check_query.format(config_data.biquery_project_name,config_data.bigquery_emea_propertyid))
        if (apj_count["Records"][0]==0 or apj_count["Records"][0] is None) or (lam_count["Records"][0]==0 or lam_count["Records"][0] is None) or (emea_count["Records"][0]==0 or emea_count["Records"][0] is None):
            raise Exception ("There are no records in the raw sessions table for the specified report")
        else:
            logging.info(" Data is populated into Bigquery session tables")

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

def generate_query(standard_columns,schema,table_name):
    columns=custom_dimensions_columns=product_columns=promotion_columns=experiment_columns=hits_columns=unnest_products=unnest_promotions=unnest_experiment=unnest_hits=transformed_columns=apj_local_datetime=lam_local_datetime=emea_local_datetime=apj_local_date=lam_local_date=emea_local_date=''
    for column_name in standard_columns:
        for column in schema:
            if column_name==column['column_name']:
                if column['transformation']=='' or column['transformation']==None:
                    if column['nested']=='no':
                        columns+=column['path']+' AS '+column['column_name']+','+'\n'+'\t'
                    else:
                        if column['field_name']=='hits.customDimensions':
                            value=column['value']
                            index=column['index']
                            column_name=column['column_name']
                            custom_dimensions_columns+="("+f"SELECT {value} FROM UNNEST(hits.customDimensions) WHERE index = {index}) AS {column_name}"+','+'\n'+'\t'
                        elif column['field_name']=='hits.product':
                            unnest_products=''
                            product_columns+='product.'+column['path']+' AS '+column['column_name']+','+'\n'+'\t'
                            unnest_products+=",UNNEST(hits.product) AS product"
                        elif column['field_name']=='hits.promotion':
                            unnest_promotions=''
                            promotion_columns+='promotion.'+column['path']+' AS '+column['column_name']+','+'\n'+'\t'
                            unnest_promotions+=",UNNEST(hits.promotion) AS promotion"
                        elif column['field_name']=='hits.experiment':
                            unnest_experiment=''
                            experiment_columns+='experiment.'+column['path']+' AS '+column['column_name']+','+'\n'+'\t'
                            unnest_experiment+=",UNNEST(hits.experiment) AS experiment"
                        else:
                            unnest_hits=''
                            hits_columns+='hits.'+column['path']+' AS '+column['column_name']+','+'\n'+'\t'
                            unnest_hits+=",UNNEST(hits) AS hits"
                else:
                    if column['column_name']=='local_datetime':
                        exec(column['transformation'])
                        lam_local_datetime+=eval(column['column_name']+f"({config_data.time_zone['LAM']['countries']},{config_data.time_zone['LAM']})")+' AS '+column['column_name']+','+'\n'+'\t'
                        apj_local_datetime+=eval(column['column_name']+f"({config_data.time_zone['APJ']['countries']},{config_data.time_zone['APJ']})")+' AS '+column['column_name']+','+'\n'+'\t'
                        emea_local_datetime+=eval(column['column_name']+f"({config_data.time_zone['EMEA']['countries']},{config_data.time_zone['EMEA']})")+' AS '+column['column_name']+','+'\n'+'\t'
                    elif column['column_name']=='local_date':
                        exec(column['transformation'])
                        lam_local_date+=eval(column['column_name']+f"({config_data.time_zone['LAM']['countries']},{config_data.time_zone['LAM']})")+' AS '+column['column_name']+','+'\n'+'\t'
                        apj_local_date+=eval(column['column_name']+f"({config_data.time_zone['APJ']['countries']},{config_data.time_zone['APJ']})")+' AS '+column['column_name']+','+'\n'+'\t'
                        emea_local_date+=eval(column['column_name']+f"({config_data.time_zone['EMEA']['countries']},{config_data.time_zone['EMEA']})")+' AS '+column['column_name']+','+'\n'+'\t'
                    else:
                        exec(column['transformation'])
                        transformed_columns+=eval(column['column_name']+"()")+' AS '+column['column_name']+','+'\n'+'\t'
    query=f"IF NOT EXISTS (SELECT size_bytes FROM `ww-ga360-bigquery-stellar-bi.MVP_New.__TABLES__` WHERE table_id= '{table_name}') \nTHEN \nCREATE OR REPLACE TABLE `ww-ga360-bigquery-stellar-bi.MVP_New.{table_name}`\nPARTITION BY date\nCLUSTER BY country,site_sub_type,local_date AS\n(SELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{apj_local_date}{apj_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.165563282.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')\nUNION ALL\nSELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{emea_local_date}{emea_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.165520044.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')\nUNION ALL\nSELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{lam_local_date}{lam_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.159417159.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')); \nELSE \nINSERT INTO `ww-ga360-bigquery-stellar-bi.MVP_New.{table_name}`\n (SELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{apj_local_date}{apj_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.165563282.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')\nUNION ALL\nSELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{emea_local_date}{emea_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.165520044.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')\nUNION ALL\nSELECT {columns}{custom_dimensions_columns}{product_columns}{promotion_columns}{experiment_columns}{hits_columns}{transformed_columns}{lam_local_date}{lam_local_datetime}clientId AS client_id\n\t\rFROM `ww-ga-360-big-query.159417159.ga_sessions_*`{unnest_hits}{unnest_products}{unnest_promotions}{unnest_experiment}\nWHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')); END IF;"
    return query

def success_notification(notification_name,report_link):

    Date=datetime.now()
    Date=Date.strftime('%Y-%m-%d')

    myTeamsMessage = pymsteams.connectorcard(report_link)
    myTeamsMessage.title(notification_name)
    myTeamsMessage.text("""**""" + Date + """**""")
    Section = pymsteams.cardsection()
    Section.title("""**Ran Successfully**""")
    myTeamsMessage.addSection(Section)
    myTeamsMessage.color("C6EFCE")
    myTeamsMessage.send()