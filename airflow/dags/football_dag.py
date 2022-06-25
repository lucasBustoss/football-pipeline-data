import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import datetime
from pathlib import Path
from os.path import join

from airflow.models import DAG
from airflow.operators.football import ApiFootballOperator, OddsPortalOperator, WhoScoredOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 4, 10)
}
BASE_WRITE_FOLDER = join(
    str(Path('/home/lucas').expanduser()),
    'pipeline-data/datalake/{stage}/{partition}/{source}'
)
BASE_READ_FOLDER = join(
    str(Path('/home/lucas').expanduser()),
    'pipeline-data/datalake/{stage}/{partition}'
)
PARTITION_FOLDER = 'extract_date={{ ds }}'

with DAG(
    dag_id='football_dag', 
    default_args=ARGS,
    schedule_interval='@daily',
    max_active_runs=1
    ) as dag:
    api_football_operator = ApiFootballOperator(
        task_id='api_football',
        file_path=join(
            BASE_WRITE_FOLDER.format(stage='bronze', source='api_football', partition=PARTITION_FOLDER),
            "ApiFootball_{{ ds_nodash }}.json"
        )
    )
    odds_portal_operator = OddsPortalOperator(
        task_id='odds_portal',
        file_path=join(
            BASE_WRITE_FOLDER.format(stage='bronze', source='odds_portal', partition=PARTITION_FOLDER),
            "OddsPortal_{{ ds_nodash }}.json"
        )
    )
    who_scored_operator = WhoScoredOperator(
        task_id='who_scored',
        file_path=join(
            BASE_WRITE_FOLDER.format(stage='bronze', source='who_scored', partition=PARTITION_FOLDER),
            "WhoScored_{{ ds_nodash }}.json"
        )
    )
    football_transform = SparkSubmitOperator(
        task_id='transform_football',
        application=join(
            str(Path(__file__).parents[2]),
            'spark/transformation.py'
        ),
        name='football_transformation', 
        application_args=[
            '--src',
            BASE_READ_FOLDER.format(stage='bronze', partition=PARTITION_FOLDER),
            '--dest',
            BASE_WRITE_FOLDER.format(stage='silver', source="", partition=""),
            '--process-date',
            '{{ ds }}'
        ]
    )
    football_get_matchs = SparkSubmitOperator(
        task_id='football_get_matchs',
        application=join(
            str(Path(__file__).parents[2]),
            'spark/get_matchs.py'
        ),
        name='football_get_matchs'
    )
    football_get_stats = SparkSubmitOperator(
        task_id='football_get_stats',
        application=join(
            str(Path(__file__).parents[2]),
            'spark/get_stats.py'
        ),
        name='football_get_stats'
    )

    api_football_operator >> football_transform >> football_get_matchs
    odds_portal_operator >> football_transform >> football_get_matchs
    who_scored_operator >> football_transform >> football_get_stats