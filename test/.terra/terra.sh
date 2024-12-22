#!/bin/bash

# the s3 bucket should already exist.
terraform init -backend-config="bucket=yuri-terraform"

terraform plan -out terra.plan