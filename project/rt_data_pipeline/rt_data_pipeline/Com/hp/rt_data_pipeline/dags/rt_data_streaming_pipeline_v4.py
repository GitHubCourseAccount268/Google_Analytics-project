from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta
from connections.SM_connection import get_secret
import data.rt_constant as constant

import os
import json
import traceback
import pandas as pd 

# try:
#     with open('dags/data/rt_constant.txt','r') as s: 
#         config_data=json.load(s)
#     with open('dags/data/rt_bq_query.txt','r') as f:
#         bq_queries=json.load(f)
#     with open('dags/data/rt_postgre_query.txt','r') as r:
#         postgre_queries=json.load(r)

# except FileNotFoundError as fn:
#     print(f'Error occured:{fn}')
#     raise fn
# except Exception as fe:
#     traceback.print_exc()
#     raise fe

email = get_secret(constant.email_secret_name)

default_args = {
    'owner': constant.owner,
    'depends_on_past': constant.depends_on_past,
    'catchup': constant.catchup,
    'email': email["email"],
    'email_on_failure': constant.email_on_failure,
    'provide_context': constant.provide_context,
    'email_on_retry': constant.email_on_retry,
    'retries': constant.retries,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id=constant.dag_id, 
    default_args = default_args,
    start_date=datetime.strptime(constant.start_date,constant.start_date_format),
    schedule_interval=constant.schedule_interval, 
    catchup=constant.catchup,
    dagrun_timeout=timedelta(minutes=3),
)

def connection_to_bq(**context):    
    from connections.GCP_BQ_connection import get_gcp_bq_client
    from google.cloud import bigquery
    from data.rt_query import extracting_from_bigquery

    export_id=list(context['ti'].xcom_pull(key='export_id',task_ids=[constant.postgre_conn_prev_export_task_id]))[0]
    try:
        if constant.use_sm_configuration:
            bigquery_client=get_gcp_bq_client(constant.use_sm_configuration,constant.gcp_secret_name)
        else:
            bigquery_client=get_gcp_bq_client(constant.use_sm_configuration,constant.gcp_conn_id)

        data_bq=pd.DataFrame()
        for key in ['apj','lam']:
            data=extracting_from_bigquery(bigquery_client,export_id,key)
            file = key+'.csv'
            if os.path.exists(file):
                os.remove(file)
            else:
                print (f"The {file} does not exist")
            data.to_csv("apj.csv",index=False)
            print(data.shape)
            data_bq = data_bq._append(data, ignore_index=True)
        # for key in bq_queries:
        #     query_job = bigquery_client.query(bq_queries[key].format(export_id))
        #     data=query_job.result()
        #     data = data.to_dataframe()
        #     file = key+'.csv'
        #     if os.path.exists(file):
        #         os.remove (file)
        #     else:
        #         print ("The {} does not exist".format(file))
        #     data.to_csv(file,index=False)
        data_bq['Quantity_Added_to_Cart']=data_bq['Quantity_Added_to_Cart'].fillna(0)
        data_bq['Quantity_Added_to_Cart']=data_bq['Quantity_Added_to_Cart'].astype(int)
        if os.path.exists('data_bq.csv'):
            os.remove('data_bq.csv')
        else :
            print ("The data_bq file does not exist")
        data_bq.to_csv('data_bq.csv',index=False)
        print(data_bq.shape)
        dir_list = os.listdir()
        print('List of files:-')
        print(dir_list)
            
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        bigquery_client.close()

def export_id(**context):
    from connections.Postgre_connection import get_Postgre_connection
    from data.rt_query import exporting_max_export_id

    try:
        if constant.use_sm_configuration:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.Postgre_secret_name)
        else:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.postgre_conn_id)
        export_id=exporting_max_export_id(cursor,conn)
        context['ti'].xcom_push(key='export_id',value=export_id)
        # cursor.execute(postgre_queries['export_id'])
        # a=cursor.fetchall()
        # export_id=int(a[0][0])
        # context['ti'].xcom_push(key='export_id',value=export_id)
        # conn.commit()
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        cursor.close()

def truncate_pg_intermediate(**context):
    from connections.Postgre_connection import get_Postgre_connection
    from data.rt_query import truncating_intermediate_table

    dir_list = os.listdir()
    print('List of files:-')
    print(dir_list)
    data_bq=pd.read_csv('data_bq.csv')
    print(data_bq.shape)
    if os.path.exists ('MVP_2_7_latest_export.csv') :
        os.remove ('MVP_2_7_latest_export.csv')
    else :
        print ("The file does not exist")
    data_bq.to_csv('MVP_2_7_latest_export.csv',index=False,header=False)

    try:
        if constant.use_sm_configuration:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.Postgre_secret_name)
        else:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.postgre_conn_id)
        count_mvp1=truncating_intermediate_table(cursor,conn)
        print(count_mvp1)
        # cursor.execute(postgre_queries['truncate'][0])
        # conn.commit()
        # with open("MVP_2_7_latest_export.csv",'r',encoding='UTF-8') as f:
        #     cursor.copy_expert(postgre_queries['truncate'][1], f)
        # cursor.execute(postgre_queries['truncate'][2])
        # count_mvp=cursor.fetchall()
        # count_mvp1=int(count_mvp[0][0])
        # print(count_mvp1)
        # conn.commit()
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        cursor.close()
        
def choose_task(**kwargs):
    data_bq=pd.read_csv('data_bq.csv')

    if data_bq.empty:
        return 'no_new_data_msg'
    else:
        return 'trunc_inter_pg'

def delete_old_data(**context):
    from connections.Postgre_connection import get_Postgre_connection
    from data.rt_query import deleting_old_data
    
    try:
        if constant.use_sm_configuration:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.Postgre_secret_name)
        else:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.postgre_conn_id)

        deleting_old_data(cursor,conn)
        # with open("dags/data/delete_old_data_query.txt",'r') as f:
        #     query=f.read()
        # cursor.execute(query)
        # conn.commit()
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        cursor.close()
    
def insert_new_rows(**context):
    from connections.Postgre_connection import get_Postgre_connection
    from data.rt_query import inserting_new_rows

    try:
        if constant.use_sm_configuration:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.Postgre_secret_name)
        else:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.postgre_conn_id)

        inserting_new_rows(cursor,conn)
        # with open("dags/data/insert_new_rows_query.txt",'r') as f:
        #     query=f.read()
        # cursor.execute(query)
        # conn.commit()
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        cursor.close()

def delete_past_data(**context):
    from connections.Postgre_connection import get_Postgre_connection
    from data.rt_query import deleting_past_data

    try:
        if constant.use_sm_configuration:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.Postgre_secret_name)
        else:
            conn,cursor =get_Postgre_connection(constant.use_sm_configuration,constant.postgre_conn_id)
    
        for key in ["in","au","id","sg","kr","my","cn","th","hk","ar","br","cl","co","mx","pe"]:
            deleting_past_data(cursor,conn,key)
            
        # for key in postgre_queries['past_data']:
        #     cursor.execute(postgre_queries['past_data'][key][0])
        #     Date=cursor.fetchall()
        #     Date=int(Date[0][0])
        #     cursor.execute(postgre_queries['past_data'][key][1],{'int':Date})
        #     conn.commit()
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except LookupError as le:
        print(f'Key Index error occured:{le}')
        raise le
    except FileNotFoundError as fe:
        print(f"The file was not found: {fe}")
        raise fe
    except Exception as e:
        traceback.print_exc()
        raise e
    finally:
        cursor.close()

def success_notification(**context):
    import pymsteams
    from connections.SM_connection import get_secret

    info = get_secret(constant.Teams_secret_name)
    data_bq=pd.read_csv('data_bq.csv')
    Date=datetime.now().strftime('%Y-%m-%d')
    shape=data_bq.shape[0]
    myTeamsMessage = pymsteams.connectorcard(info["reports_link"])
    myTeamsMessage.title("Real Time Data Streaming Pipeline V4 Log")
    myTeamsMessage.text("""**""" + Date + """**""")
    Section = pymsteams.cardsection()
    Section.title("""**Ran Successfully**""")
    Section.text('Number of Rows Updated : {}'.format(shape))
    myTeamsMessage.addSection(Section)
    myTeamsMessage.color("C6EFCE")
    myTeamsMessage.send()

# cmd = """
# pip3 install google-cloud
# pip3 install google-cloud-bigquery
# pip3 install pymsteams
# pip3 install google-api-python-client
# pip3 install pandas
# pip3 install psycopg2-binary
# pip3 install cryptography
# pip3 install pycryptodomex
# pip3 install google-api-python-client
# pip3 install --upgrade pandas
# pip3 install db-dtypes
# """

# run_this = BashOperator(
#     task_id=constant.Pip_valicurr_date_task_id,
#     bash_command=cmd,
#     dag = dag
# )

postgre_conn_prev_export_id=PythonOperator(task_id=constant.postgre_conn_prev_export_task_id,python_callable=export_id,dag=dag)
data_push_transform_create_local_copy=PythonOperator(task_id=constant.data_push_transform_create_local_copy_task_id,python_callable=connection_to_bq,dag=dag)
branch_task=BranchPythonOperator(task_id=constant.branch_task_id,python_callable=choose_task,dag=dag)
no_new_data_msg=PythonOperator(task_id=constant.no_new_data_msg_task_id,python_callable=success_notification,dag=dag)
trunc_inter_pg=PythonOperator(task_id=constant.trunc_inter_pg_task_id,python_callable=truncate_pg_intermediate,dag=dag)
delete_current_day_data_final=PythonOperator(task_id=constant.delete_current_day_data_final_task_id,python_callable=delete_old_data,dag=dag)
insert_current_day_data_final=PythonOperator(task_id=constant.insert_current_day_data_final_task_id,python_callable=insert_new_rows,dag=dag)
delete_past_data_final=PythonOperator(task_id=constant.delete_past_data_final_task_id,python_callable=delete_past_data,dag=dag)
data_entry_msg=PythonOperator(task_id=constant.data_entry_msg_task_id,python_callable=success_notification,dag=dag)
    

postgre_conn_prev_export_id >> data_push_transform_create_local_copy >> branch_task >> no_new_data_msg
postgre_conn_prev_export_id >> data_push_transform_create_local_copy >> branch_task >> trunc_inter_pg >> \
    delete_current_day_data_final >> insert_current_day_data_final >> delete_past_data_final >> data_entry_msg
