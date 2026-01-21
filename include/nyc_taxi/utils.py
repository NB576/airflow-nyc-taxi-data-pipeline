from airflow.models import Connection

def get_storage_options(conn_id: str = "aws_default"):
    conn = Connection.get_connection_from_secrets('aws_default')
    storage_options = {
            "key": conn.login,
            "secret": conn.password,
            "token": conn.extra_dejson.get('session_token')
        }
    return storage_options