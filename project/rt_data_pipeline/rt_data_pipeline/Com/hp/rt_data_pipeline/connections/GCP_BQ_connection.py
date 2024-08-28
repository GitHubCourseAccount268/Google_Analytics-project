from google.cloud import bigquery
from google.cloud import exceptions
from connections.GCP_connection import get_gcp_connection
import traceback

def get_gcp_bq_client(sm_config,gcp_connection_req):
    credentials = get_gcp_connection(sm_config,gcp_connection_req)
    try:
        bigquery_client= bigquery.Client(credentials=credentials, project=credentials.project_id)
        print("bq connection created")
        return bigquery_client
    except exceptions.Forbidden:
        raise ValueError("ERROR: Connection Error")
    except Exception as e:
        traceback.print_exc()
        raise e