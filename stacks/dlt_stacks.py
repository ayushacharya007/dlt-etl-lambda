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
        
        custom_dlt_layer = _lambda.LayerVersion(
            self, "CustomDltAthenaLayer",
            layer_version_name  ="custom-dlt-athena-layer-v1",
            code=_lambda.Code.from_asset("layers/"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            compatible_architectures=[_lambda.Architecture.ARM_64],
            description="Custom DLT Layer for Athena",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Define the Lambda function
        etl_function = _lambda.Function(
            self, "EtlFunction",
            function_name="dlt_weather_etl",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="extract_load_lambda.handler",
            code=_lambda.Code.from_asset("lambda/"),
            description="Function to extract and load weather data",
            timeout=Duration.minutes(5),
            memory_size=512,
            architecture=_lambda.Architecture.ARM_64,
            layers=[custom_dlt_layer],
            environment={
                "WEATHER_API_KEY": os.environ["WEATHER_API_KEY"]
            }
        )

        # Grant S3 permissions
        data_bucket.grant_read_write(etl_function)
        query_results_bucket.grant_read_write(etl_function)

        # Grant Athena permissions
        etl_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:GetWorkGroup",
                "athena:GetDataCatalog",
                "athena:ListQueryExecutions",
            ],
            resources=[
                f"arn:aws:athena:{self.region}:{self.account}:workgroup/*",
                f"arn:aws:athena:{self.region}:{self.account}:datacatalog/*",
            ],
        ))

        # Grant Glue permissions for database and table operations
        etl_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "glue:CreateDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:UpdateTable",
                # "glue:GetPartition",
                # "glue:GetPartitions",
                # "glue:CreatePartition",
                # "glue:BatchCreatePartition",
                # "glue:UpdatePartition",
                # "glue:BatchUpdatePartition",
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:database/*",
                f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
            ],
        ))

        # Grant Lake Formation permissions (if Lake Formation is enabled)
        etl_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "lakeformation:GetResourceLFTags",
                "lakeformation:GetDataAccess",
            ],
            resources=["*"],
        ))
        
        # Define the EventBridge rule to run the Lambda function every hour
        rule = events.Rule(
            self, "Rule",
            schedule=events.Schedule.cron(minute="0", hour="*"), 
            targets=[events_targets.LambdaFunction(etl_function)] #type: ignore
        )
