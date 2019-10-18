#!/usr/bin/env bash

# Simply run this script without parameters
# it will download the CSV file required for the Spark example to run

set -e

BASE_DIR=$(dirname $0)
ROOT_DIR="$BASE_DIR/.."
mkdir -p $ROOT_DIR/data/in/au-domestic-airlines

cd $ROOT_DIR/data/in/au-domestic-airlines
wget https://data.gov.au/data/dataset/29128ebd-dbaa-4ff5-8b86-d9f30de56452/resource/cf663ed1-0c5e-497f-aea9-e74bfda9cf44/download/otp_time_series_web.csv
