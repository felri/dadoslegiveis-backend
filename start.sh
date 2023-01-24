#!/bin/bash

export DB_HOST="database-1.cz5u7z4zqzhn.us-east-1.rds.amazonaws.com"
export DB_PASSWORD="dB1UjoDcnxQm6dR8MvOJuB+4y0UMf6"
export DB_NAME="baseproject"
export DB_USER="postgres"



hypercorn main:app --bind 0.0.0.0:5001 --reload