from google.cloud import exceptions
import traceback
from google.oauth2 import service_account
from connections.SM_connection import get_secret

def get_gcp_connection(sm_config,gcp_connection_req):
    try:
        if sm_config:
            info = get_secret(gcp_connection_req)
            credentials = service_account.Credentials.from_service_account_info(info)
        else:
            from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
            conn = GoogleCloudBaseHook(gcp_conn_id=gcp_connection_req)
            credentials = conn._get_credentials()
        return credentials
    except exceptions.Forbidden:
        raise ValueError("ERROR: Connection Error")
    except Exception as e:
        traceback.print_exc()
        raise e