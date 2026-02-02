from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from include.nyc_taxi.tasks_dynamic import generate_monthly_dates_2024, run_data_quality_checks, run_transform_to_staging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.nyc_taxi.constants import S3_BUCKET, BROWSER_HEADERS

import requests

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='0 0 1 * *',
    catchup=False,
    description='extract data from api and load into s3'
)
def nyc_taxi_dynamic():

    @task
    def get_monthly_dates():
        return generate_monthly_dates_2024()

    @task_group
    def monthly_pipeline(year_month):

        @task
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
                s3_hook.load_file_obj(  # Streams response.raw directly to S3
                    file_obj=response.raw,
                    key=key,
                    bucket_name='nyc-taxi-project-112025',
                    replace=True)
            else:
                print(f"{key} already present in bucket {S3_BUCKET}")
        
        # check whether data is within acceptability thresholds to proceed
        @task
        def quality_check_raw_data(year_month):
            return run_data_quality_checks(year_month)
        
        # perform basic transformations on raw data, upload to staging dir
        @task
        def transform_raw_to_staging(year_month, s3_key):
            run_transform_to_staging(year_month, s3_key)

        #link tasks in taskgroup 
        url_task = get_url(year_month)
        upload_task = upload_to_s3(year_month, url_task)
        quality_check_task = quality_check_raw_data(year_month)
        transform_to_staging_task = transform_raw_to_staging(year_month, quality_check_task)

        url_task >> upload_task >> quality_check_task >> transform_to_staging_task

        return transform_raw_to_staging
    
    year_month_list = get_monthly_dates()
    results = monthly_pipeline.expand(year_month=year_month_list)
        
nyc_taxi_dynamic()
   

            