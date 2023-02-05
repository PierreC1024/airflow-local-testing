"""BigQuery Operator DAG"""
from __future__ import annotations

import datetime
import pendulum

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dags.operator.bigquery import BigQueryInsertJobOperatorV2

location = "US"
test_query = """
    SELECT COUNT(*)
    FROM `bigquery-test-374822.public.wikipedia_pageviews_2023`
    WHERE DATE(datehour) >= "2023-01-15" AND SEARCH(title, '`paul`')
"""

with DAG(
    dag_id="bigquery_operator_test",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"google_cloud_default": "GCP_BQ_CONN"},
) as dag:

    insert_query_job = BigQueryInsertJobOperator(
        task_id="test_query",
        configuration={
            "query": {
                "query": test_query,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    insert_query_job_slots = BigQueryInsertJobOperatorV2(
        task_id="test_query_slots",
        configuration={
            "query": {
                "query": test_query,
                "useLegacySql": False,
            }
        },
        location=location,
        flex_slot_provisioning=100,
    )

    insert_query_job >> insert_query_job_slots
