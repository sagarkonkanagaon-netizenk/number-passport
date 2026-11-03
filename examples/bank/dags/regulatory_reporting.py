"""Daily regulatory reporting pipeline: bronze -> silver -> gold -> capital report."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="regulatory_reporting",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bcbs239", "capital"],
) as dag:
    build_exposures = SQLExecuteQueryOperator(
        task_id="build_exposures",
        conn_id="warehouse",
        sql="models/sql/silver_exposures.sql",
    )
    adjust_collateral = SparkSubmitOperator(
        task_id="adjust_collateral",
        application="jobs/collateral_adjust.py",
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="dbt run --select models/dbt/gold/rwa_by_class.sql",
    )
    [build_exposures, adjust_collateral] >> build_gold
