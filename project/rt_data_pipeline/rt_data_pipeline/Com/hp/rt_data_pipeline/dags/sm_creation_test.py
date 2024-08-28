import boto3
import json
from datetime import date,datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'omkar',
    'depends_on_past': False,
    'email': ["airflow@example.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}

dag = DAG(dag_id='sm_secret_creation_test', default_args = default_args, start_date=datetime.strptime('2022-1-1',"%Y-%m-%d"),
         schedule_interval="@once", catchup=False)

# def sm_create_secret():
#     client=boto3.client("secretsmanager")
#     response = client.create_secret(
#     Name='airflow/connections/airflow_gcp_service_account',
#     Description='allow access to gcp through service account',
#     SecretString='''{"type": "service_account",
#     "project_id": "ww-ga360-bigquery-stellar-bi",
#     "private_key_id": "705a40afc973559e0d09a7dc1849815b20e96aca",
#     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCzbeBD9k5hAhvE\nrO8wbZ0ZpsdN6/aVQQGPpTzqd5ViPNI/HVrTRPGS/ChvVVd1SXkTetSM+9LDO+aj\n6cUHRCc8z8tzICbJ5lD+fC7f7jBAmsiKEPfB2PyYxKJKOJd1ktOqxOiWDCNqGy3K\njzpD6uTN8Jfd+yW5Nm3x9qd0WnbPPsrtPhSx3StDa4tdnP19E82nLDIUphJlvD62\nBofxNM2Wp8+DfoIDQ4pB/hzk2JB7rhEtpHkFQIep6YJpQ8vYU18OrNkYI9/v4S1J\ntySoDotjj69ik9j+5m+3DDrntrWsUpZSHW/WsEFq3sFtoZV+saShzx8Z5z8tQ5Nk\n5eJbagMjAgMBAAECggEABGVjGDyBoRjRy1jH/MdtZkJ9duhFa1S4uryRUyvzigD0\nJGhjYEe7z3PEHdjZGAjhIYa3NxkNtIt+1ByXH4geRKbRVOnAaYlDX2jK0TZfEkYj\nNwFsFeDAwjwXFHsceDH3uPhdZnJiWmgourdq9bJclrsX+BqoyvKNcQnaV8cboBTe\n5iz6YHkgkcXyj7Wz+kgneH3DkplNhQOaTcAgN4c+KCivU7fBnobWB/fXt0aQs0Ig\nXqcfwsvxzfZUb9MTyvExa62bUzTErFaodIG5stkvIQowHMyugKEWD0vYfUgPa2Ea\ndPMpGaBbMtT8TgbWYAg3a30K9chJn7Nppb2ILw418QKBgQDmnx9M4xm0f1KIYXrV\nJ4GuhGIFWssLzZf61YJodLqKlitJ+q20EdUOpwwsLO9YknCej60PZJdmeY3KQXJN\n5XrLSUOhAAvU+EaAX/veZZG+Jzp+zCUIB22b84mIlGpfZvpvFg3miBm6JIdjSs9f\nXYHaTedHJ62Aa0tnoHKEXcPHCwKBgQDHLJryF6bCP3isM08dcSHP8/wRWgBryQQC\nL714xaNkocEAV3vTKMoRt+BLGXDLoSZV9DIE0u4EIiWpCaf+WBb3A3rqepwuSqub\nxiYQOdfy/HR3zdfIafWAh9f5u22ZsfFX9c+Kq2mzrJAodR7IWkbIg8TNc4qRNtRm\n5Lv5t/djSQKBgQDkzHsmMazf6O19xpAxhtdex5Hj1Bbbp+YfAdI2RZCIS8G4zI2m\nt4ZT2iD2dsIicm5usQY13ktibDfisBlx9LhllFMXGM+kQ4jWdeOQ2d3E9HBROcGH\nfK8e6HfLW8tIyQauTQgPbXlwtirntGGWMFPSvDU238N/Q3N05LaYYdoIzwKBgQC0\nmWSR+hIydl0UP54QQ0H2jvRUQ3i4Q1hIJ0O1m/fxSp53kdvsd0Lq2AEf95yId8IM\nFFW6fAxoYRIm+WODxBpmtpggvzaY3wpGQGDJO7ntUS7GZzavOizq88JJZsMVpv0A\n1hnvUkiRK/q3RKO55eni7WBpLshJrh7gga1U3JQMSQKBgQDkuUYKAgLMTmiEmciT\nk3mydE3PFC6LFdFquFYIEu2vI/3e0MfMieV/0StFkyEhgzQs5LqYJcFyj5+3/1K0\nG+dDKjFvQXnOgS5rYVW8RfvZKm3fUjLjFxCYEeEiUlHFVwnMJLRtIA+zRaUeO2IV\nfq9eMrHIxjO0SOXz9ZZIZ7MydA==\n-----END PRIVATE KEY-----\n",
#     "client_email": "wwga360-aws-transfer@ww-ga360-bigquery-stellar-bi.iam.gserviceaccount.com",
#     "client_id": "110364032638676401289",
#     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#     "token_uri": "https://oauth2.googleapis.com/token",
#     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/wwga360-aws-transfer%40ww-ga360-bigquery-stellar-bi.iam.gserviceaccount.com"}'''
# ,
#     Tags=[
#         {
#             'Key': 'TBAC_SG_GA_BigQuery',
#             'Value': 'True'
#         },
#     ],
#     )
def sm_create_secret():
    client=boto3.client("secretsmanager")
    response = client.create_secret(
    Name='airflow/connections/airflow_teams_notification_rt_pipeline',
    Description='Get success notification to teams',
    SecretString='{"reports_link":"https://hp.webhook.office.com/webhookb2/bb6d661b-5d22-4876-9621-6417c341ab5b@ca7981a2-785a-463d-b82a-3db87dfc3ce6/IncomingWebhook/02ecce4f994749238c9c2c14429773dc/6200eba1-e061-496e-834e-2e60ebfee2f4"}'
,
    Tags=[
        {
            'Key': 'TBAC_SG_GA_BigQuery',
            'Value': 'True'
        },
    ],
    )

def sm_secret_list():
    client=boto3.client("secretsmanager")
    response = client.list_secrets(
        Filters=[
        {
            'Key': 'name',
            'Values': [
                'airflow/connections/',
            ]
        },
        ],
    )
    print(response)

def sm_delete_secret():
    client=boto3.client("secretsmanager")
    response = client.delete_secret(
    SecretId='airflow/connections/airflow_teams_notification',
    ForceDeleteWithoutRecovery=True)
    print(response)

def sm_describe_secret():
    client=boto3.client("secretsmanager")
    response = client.describe_secret(
    SecretId='airflow/connections/airflow_teams_notification')
    print(response)

def sm_secret_extract():
    client=boto3.client("secretsmanager")
    response = client.get_secret_value(
    SecretId='airflow/connections/airflow_teams_notification',
    )
    print(f'complete response:\n {response}')
    secret = response['SecretString']

    print(secret)
    print(json.loads(secret,strict=False))

import os

def find_file():
    dir_list = os.listdir()
    # prints all files
    print(dir_list)

sm_create_secret_task=PythonOperator(task_id='sm_create_secret',python_callable=sm_create_secret,dag=dag)
#sm_list_secret_task=PythonOperator(task_id='sm_list_secret',python_callable=sm_secret_list,dag=dag)
#sm_describe_secret_task=PythonOperator(task_id='sm_describe_secret',python_callable=sm_describe_secret,dag=dag)
#sm_secret_extract_task=PythonOperator(task_id='sm_secret_extract',python_callable=sm_secret_extract,dag=dag)
# find_file_path_task=PythonOperator(task_id='find_file_path',python_callable=find_file,dag=dag)
#sm_delete_secret_task=PythonOperator(task_id='sm_delete_secret',python_callable=sm_delete_secret,dag=dag)

#sm_create_secret_task >> sm_list_secret_task >> sm_describe_secret_task >> sm_secret_extract_task #>> sm_delete_secret_task
sm_create_secret_task