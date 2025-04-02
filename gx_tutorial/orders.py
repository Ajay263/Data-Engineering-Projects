from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import great_expectations.expectations as gxe
import pandas as pd
import boto3
from io import StringIO
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext

# Define bucket names
SOURCE_BUCKET_NAME = 'nexabrand-prod-source'
DOCS_BUCKET_NAME = 'nexabrand-prod-gx-doc'
FILE_KEY = 'data/order_fulfillment.csv'


# Configuration function for checkpoint
def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
    """This function takes a GX Context and returns a Checkpoint that
    can validate data from S3 against an ExpectationSuite, and run Actions."""
    # Get S3 client
    s3_client = boto3.client('s3')
    
    # Get data from S3
    response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=FILE_KEY)
    content = response['Body'].read().decode('utf-8')
    order_fulfillment_df = pd.read_csv(StringIO(content))
    
    # Add a Pandas Data Source
    data_source = context.data_sources.add_pandas(name="order_fulfillment")
    
    # Add a Data Asset for the DataFrame
    data_asset = data_source.add_dataframe_asset(name="order_fulfillment")
    
    # Add the Batch Definition
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="order_fulfillment_batch"
    )
    
    # Create expectation suite
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name="order_fulfillment_data_expectation_suite",
            expectations=[
                # Expectation 1: Check columns
                gxe.ExpectTableColumnsToMatchSet(
    column_set=["order_id", "customer_id", "order_placement_date"],
    exact_match=False
),
               
                gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
                gxe.ExpectColumnValuesToBeUnique(column="order_id"),
          
                gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
                gxe.ExpectColumnValuesToNotBeNull(column="order_placement_date"),
            ],
        )
    )
    
    # Create validation definition
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="order_fulfillment",
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    
    # Configure S3 Data Docs site if needed
    s3_site_name = "s3_data_docs"
    if s3_site_name not in context.list_data_docs_sites():
        s3_site_config = {
            "class_name": "SiteBuilder",
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
            },
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": DOCS_BUCKET_NAME,
                "prefix": "",
            },
        }
        context.add_data_docs_site(site_name=s3_site_name, site_config=s3_site_config)
    
    # Set up actions for the checkpoint
    from great_expectations.checkpoint import SlackNotificationAction, UpdateDataDocsAction
    
    action_list = [
        SlackNotificationAction(
            name="Great Expectations data quality results",
            slack_webhook="https://hooks.slack.com/services/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",
            notify_on="failure",
            show_failed_expectations=True,
        ),
        UpdateDataDocsAction(
            name="update_all_data_docs",
            site_names=[s3_site_name],
        ),
    ]
    
    # Create and return checkpoint
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="order_fulfillment_checkpoint",
            validation_definitions=[validation_definition],
            actions=action_list,
            result_format={"result_format": "COMPLETE"},
        )
    )
    
    return checkpoint


# DAG definition
with DAG(
    dag_id="order_fulfillment_gx_validation",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["great_expectations", "data_quality"],
) as dag:
    
    @task
    def fetch_data_from_s3():
        """Task to fetch data from S3 and return it as a dataframe"""
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=FILE_KEY)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        return df
    
    # Task to validate data with Great Expectations
    validate_order_fulfillment = GXValidateCheckpointOperator(
        task_id="validate_order_fulfillment",
        configure_checkpoint=configure_checkpoint,
        # We don't need to pass batch_parameters here as the checkpoint function handles it
    )
    
    @task.short_circuit()
    def check_validation_results(task_instance):
        """Task to check if validation passed"""
        result = task_instance.xcom_pull(task_ids="validate_order_fulfillment")
        return result.get("success")
    
    @task
    def build_data_docs():
        """Task to build data docs in S3"""
        import great_expectations as gx
        context = gx.get_context(mode='file', project_root_dir="./great_expectations")
        context.build_data_docs(site_names=["s3_data_docs"])
        
        # Generate the S3 website URL
        s3_client = boto3.client('s3')
        region = s3_client.meta.region_name
        website_url = f"http://{DOCS_BUCKET_NAME}.s3-website-{region}.amazonaws.com/"
        return website_url
    
    @task
    def success_notification(website_url):
        """Task to notify about successful validation"""
        print(f"Data validation successful! Reports available at: {website_url}")
    
    # Define the DAG tasks dependencies
    data = fetch_data_from_s3()
    validation_success = check_validation_results()
    docs_url = build_data_docs()
    notification = success_notification(docs_url)
    
    # Set up the task dependencies
    chain(
        data,
        validate_order_fulfillment,
        validation_success,
        docs_url,
        notification,
    )