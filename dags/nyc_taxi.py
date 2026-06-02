from airflow.decorators import dag, task, task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import AthenaAccessKeyProfileMapping
from include.nyc_taxi.tasks.raw import generate_url, upload_to_s3, get_formatted_monthly_dates, run_data_quality_checks
from include.nyc_taxi.constants import YEAR
from include.nyc_taxi.config.spark import SPARK_CONF
from pendulum import datetime
from datetime import timedelta

from pathlib import Path

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description=''
)
def nyc_taxi():

    @task
    def get_monthly_dates():
        return get_formatted_monthly_dates(YEAR)

    @task_group
    def raw_to_staging(year_month):

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def get_url(year_month):
            return generate_url(year_month)
        
        @task
        def upload_to_bucket(url, year_month):
            upload_to_s3(year_month, url)
        
        @task
        def quality_check_raw_data(year_month):
            return run_data_quality_checks(year_month)
        
        url_task = get_url(year_month)
        upload_task = upload_to_bucket(url_task, year_month)
        quality_check_task = quality_check_raw_data(year_month)

        url_task >> upload_task >> quality_check_task
    
    dag_dir = Path(__file__).parent
    project_root = dag_dir.parent
    staging_job_path = project_root/"include"/"nyc_taxi"/"jobs"/"staging_spark_job.py"


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
        conf=SPARK_CONF
    )


    curated_dbt_taskgroup = DbtTaskGroup(
        group_id="curated_dbt",
        project_config=ProjectConfig(
            dbt_project_path=str(project_root/"nyc_taxi_dbt"),
        ),
        profile_config=ProfileConfig(
            profile_name="nyc_taxi_dbt",
            target_name="dev",
            profile_mapping=AthenaAccessKeyProfileMapping(
                conn_id="aws_default",
                profile_args={
                    "region_name": "us-east-1",
                    "s3_staging_dir": "s3://nyc-taxi-project-112025/athena-results/",
                    "s3_data_dir": "s3://nyc-taxi-project-112025/curated/",
                    "schema": "nyc_taxi_curated",
                    "database": "awsdatacatalog",
                }
            )
        ),
        operator_args={ #TBD: refactor this to allow any two dates to be passed as args (currently hardcoded for full year processing)
            "vars": f'{{"start_date": "{YEAR}-01-01", "end_date": "{YEAR}-12-31"}}'
        },
    )


    year_month_list_task = get_monthly_dates()
    raw_to_staging_taskgroup = raw_to_staging.expand(year_month=year_month_list_task)

    raw_to_staging_taskgroup >> staging_transform >> curated_dbt_taskgroup

nyc_taxi()
   

            