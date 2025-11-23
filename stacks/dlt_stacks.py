from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
)
from constructs import Construct
import os
from dotenv import load_dotenv

load_dotenv()

class WeatherELTStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define the Lambda function
        etl_function = _lambda.DockerImageFunction(
            self, "EtlFunction",
            code=_lambda.DockerImageCode.from_image_asset("lambda"),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "WEATHER_API_KEY": os.environ["WEATHER_API_KEY"]
            }
        )
