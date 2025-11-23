from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
)
from constructs import Construct
import os
from dotenv import load_dotenv

load_dotenv()

class WeatherELTStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import existing S3 buckets to store data and query results
        data_bucket_name = "demo-dlt-bucket" # Use your own bucket name
        query_results_bucket_name = "aws-athena-query-results-180294223557-ap-southeast-2" # Use your own bucket name

        data_bucket = s3.Bucket.from_bucket_name(self, "DataBucket", data_bucket_name)
        query_results_bucket = s3.Bucket.from_bucket_name(self, "QueryResultsBucket", query_results_bucket_name)

        # Define the Lambda function
        etl_function = _lambda.DockerImageFunction(
            self, "EtlFunction",
            code=_lambda.DockerImageCode.from_image_asset("lambda"),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "WEATHER_API_KEY": os.environ["WEATHER_API_KEY"],
                "DESTINATION__FILESYSTEM__BUCKET_URL": f"s3://{data_bucket_name}",
                "DESTINATION__ATHENA__QUERY_RESULT_BUCKET": f"s3://{query_results_bucket_name}",
                "DESTINATION__ATHENA__FORCE_ICEBERG": "true",
            }
        )

        # Grant permissions
        data_bucket.grant_read_write(etl_function)
        query_results_bucket.grant_read_write(etl_function)

        # Grant permissions
        etl_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "glue:CreateDatabase",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetTable",
            ],
            resources=["*"]
        ))
        
        # Define the EventBridge rule to run the Lambda function every hour
        rule = events.Rule(
            self, "Rule",
            schedule=events.Schedule.cron(minute="0", hour="*"), 
            targets=[events_targets.LambdaFunction(etl_function)]
        )
