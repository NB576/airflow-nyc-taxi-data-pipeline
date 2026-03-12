from airflow.decorators import dag, task, task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from include.nyc_taxi.tasks.raw import generate_url, upload_to_s3, generate_monthly_dates, run_data_quality_checks
from include.nyc_taxi.constants import S3_BUCKET, BROWSER_HEADERS, YEAR
from pendulum import datetime
from datetime import timedelta

from pathlib import Path

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='0 0 1 * *',
    catchup=False,
    description=''
)
def nyc_taxi():

    @task
    def get_monthly_dates():
        return generate_monthly_dates(YEAR)

    @task_group
    def raw_to_staging(year_month):

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def get_url(year_month):
            return generate_url(year_month)
        
        @task
        def upload_to_bucket(url, year_month):
            upload_to_s3(year_month, url)
        
        # checks data quality in raw/ folder 
        @task
        def quality_check_raw_data(year_month):
            return run_data_quality_checks(year_month)
        

        #link tasks in taskgroup 
        url_task = get_url(year_month)
        upload_task = upload_to_bucket(url_task, year_month)
        quality_check_task = quality_check_raw_data(year_month)

        url_task >> upload_task >> quality_check_task
    
    dag_dir = Path(__file__).parent
    project_root = dag_dir.parent
    staging_job_path = project_root/"include"/"nyc_taxi"/"jobs"/"staging_spark_job.py"
    curated_job_path = project_root/"include"/"nyc_taxi"/"jobs"/"curated_spark_job.py"

    conn = BaseHook.get_connection("aws_default")

    staging_transform = SparkSubmitOperator(
        task_id="staging_transform",
        application=str(staging_job_path.resolve()),
        conn_id="spark_default",
        application_args=[
            str(YEAR)
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
        },
        conf={
        "spark.master": "local[4]",
        # let Spark's memory manager handle the internal memory division between driver and executor
        "spark.driver.memory": "5g", 
        # caps result collection — kept low as pipeline has no collect/count operations
        "spark.driver.maxResultSize": "2g",
        # overrides the default S3A auth order with AWS SDK's official sequence of credential sources so that env vars checked first
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        # stream data as its generated instead of buffering it all first
        "spark.hadoop.fs.s3a.fast.upload": "true",
        # use disk as upload buffer instead of RAM (required due to local machine resource constraints)
        "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
        # uploads in 100MB chunks — more reliable than one giant upload
        "spark.hadoop.fs.s3a.multipart.size": "104857600",
        # allows more concurrent S3 connections for faster parallel writes
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        # optimise query performance using spark adaptive sql
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    )

    curated_transform = SparkSubmitOperator(
        task_id="curated_transform",
        application=str(curated_job_path.resolve()),
        conn_id="spark_default",
        application_args=[
            str(YEAR)
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
        },
        conf={ # same conf as staging spark operator
        "spark.master": "local[4]",
        "spark.driver.memory": "5g",
        "spark.driver.maxResultSize": "2g",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
        "spark.hadoop.fs.s3a.multipart.size": "104857600",
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    )

    year_month_list_task = get_monthly_dates()
    raw_to_staging_taskgroup = raw_to_staging.expand(year_month=year_month_list_task)

    raw_to_staging_taskgroup >> staging_transform >> curated_transform 
           
nyc_taxi()
   

            