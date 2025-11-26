#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.dlt_stacks import WeatherELTStack


app = cdk.App()
WeatherELTStack(app, "WeatherELTStack",description="Stack for Weather ETL")

app.synth()
