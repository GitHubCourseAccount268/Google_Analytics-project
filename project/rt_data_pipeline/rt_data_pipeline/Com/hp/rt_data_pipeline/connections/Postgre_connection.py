import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import traceback
from connections.SM_connection import get_secret

def get_Postgre_connection(sm_config,postgre_connection_req):
    try:
        if sm_config:
            info = get_secret(postgre_connection_req)
            conn = psycopg2.connect(info['conn_string'])
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
        else:
            from airflow.hooks.base_hook import BaseHook
            conn = BaseHook.get_connection(postgre_connection_req)
            name = "postgresql"
            host = conn.host
            port = conn.port
            user = conn.login
            password = conn.get_password()
            db = "stellar_db"
            conn_string=name+'://'+user+':'+ password + '@' + host +':' + str(port)+ '/' + db
            conn = psycopg2.connect(conn_string)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
        return conn,cursor
    except LookupError as ve:
        print(f'Key value error occured:{ve}')
        raise ve
    except ConnectionError as ce:
        print(f'Connection error occured:{ce}')
        raise ce
    except Exception as e:
        traceback.print_exc()
        raise e
    
    