from airflow.models import Connection
from s3fs import S3FileSystem

default_conn = Connection.get_connection_from_secrets('aws_default')

s3_fs = S3FileSystem(
    key=default_conn.login,
    secret=default_conn.password,
    token=default_conn.extra_dejson.get('session_token')
)