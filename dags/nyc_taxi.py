from airflow.decorators import dag, task, task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from include.nyc_taxi.tasks.raw import generate_monthly_dates, run_data_quality_checks
from include.nyc_taxi.tasks.staging import staging_transform
from include.nyc_taxi.tasks.curated import curated_transform
from include.nyc_taxi.constants import S3_BUCKET, BROWSER_HEADERS, START_DATE, END_DATE
from pendulum import datetime
from datetime import timedelta

from pathlib import Path
import requests

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='0 0 1 * *',
    catchup=False,
    description=''
)
def nyc_taxi():

    @task
    def get_monthly_dates():
        return generate_monthly_dates(START_DATE, END_DATE)

    @task_group
    def raw_to_staging(year_month):

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def get_url(year_month):
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
       
            response = requests.head(url=url, headers=BROWSER_HEADERS)
            # print(url, response.status_code)
            if response.status_code == 200:
               return url
            
            raise ValueError(f"resource unavailable for {year_month}")
        
        @task
        def upload_to_s3(year_month, url):
            s3_hook = S3Hook("aws_default")
            split = year_month.split("-")
            key = f"raw/{split[0]}/{split[1]}/yellow_tripdata_{year_month}.parquet"
            
            if not s3_hook.check_for_key(key=key, bucket_name=S3_BUCKET):
                response = requests.get(url, stream=True)
                response.raw.decode_content = True #ensures decompression
                s3_hook.load_file_obj(  # Streams response.raw directly to S3
                    file_obj=response.raw,
                    key=key,
                    bucket_name=S3_BUCKET,
                    replace=True)
            else:
                print(f"{key} already present in bucket {S3_BUCKET}")
        
        # check whether data is within acceptability thresholds to proceed
        @task
        def quality_check_raw_data(year_month):
            return run_data_quality_checks(year_month)
        

        #link tasks in taskgroup 
        url_task = get_url(year_month)
        upload_task = upload_to_s3(year_month, url_task)
        quality_check_task = quality_check_raw_data(year_month)

        url_task >> upload_task >> quality_check_task
    
    dag_dir = Path(__file__).parent
    project_root = dag_dir.parent
    staging_job_path = project_root/"include"/"nyc_taxi"/"jobs"/"staging_spark_job.py"
    curated_job_path = project_root/"include"/"nyc_taxi"/"jobs"/"curated_spark_job.py"

    conn = BaseHook.get_connection("aws_default")

    staging_transform = SparkSubmitOperator(
        task_id="staging_transform",
        application=str(curated_job_path.resolve()),
        conn_id="spark_default",
        application_args=[
            START_DATE.strftime("%Y-%m-%d")
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
        },
        conf={
            # Overrides the default S3A auth order with AWS SDK's official sequence of credential sources
            # (environment vars checked first)
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.executor.memory": "4g",
            "spark.sql.adaptive.enabled": "true", 
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    )

    curated_transform = SparkSubmitOperator(
        task_id="curated_transform",
        application=str(curated_job_path.resolve()),
        conn_id="spark_default",
        application_args=[
            START_DATE.strftime("%Y-%m-%d"),
            END_DATE.strftime("%Y-%m-%d")
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.password,
        },
        conf={
            # Overrides the default S3A auth order with AWS SDK's official sequence of credential sources
            # (environment vars checked first)
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.executor.memory": "4g",
            "spark.sql.adaptive.enabled": "true", 
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    )

    year_month_list_task = get_monthly_dates()
    raw_to_staging_taskgroup = raw_to_staging.expand(year_month=year_month_list_task)

    raw_to_staging_taskgroup >> staging_transform >> curated_transform 
        
nyc_taxi()
   

            