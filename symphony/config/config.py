import logging
import configparser
import symphony
from pathlib import Path
from typing import List
import os
from distutils.util import strtobool
import pandas as pd
pd.options.mode.chained_assignment = None

# Logging settings
LOG_LEVEL = logging.DEBUG

# Pandas / Modin settings
USE_MODIN = False
if USE_MODIN:
    os.environ["MODIN_ENGINE"] = "ray"

# Config settings
module_path = symphony.__file__
path = Path(module_path)
TRADING_LIB_DIR = str(path.parent.parent) + "/"
SYMPHONY_DIR = str(path.parent.parent) + "/symphony/"
HISTORICAL_DATA_DIR = TRADING_LIB_DIR + "data/"
BACKTEST_DIR = str(path.parent) + "/backtest/"
config = configparser.ConfigParser()
config.read(TRADING_LIB_DIR + "config.ini")

# Set AWS keys
if config["aws"]["access_key"]:
    os.environ["AWS_ACCESS_KEY_ID"] = config["aws"]["access_key"]
if config["aws"]["secret_access_key"]:
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws"]["secret_access_key"]
AWS_REGION = config["aws"]["region"]
AWS_ACCESS_KEY_ID = config["aws"]["access_key"]
AWS_SECRET_ACCESS_KEY = config["aws"]["secret_access_key"]

# DynamoDB Settings
DYNAMODB_HOST = config["dynamodb"]["host"]
if not DYNAMODB_HOST.startswith("http") or not DYNAMODB_HOST.startswith("https"):
    DYNAMODB_HOST = "http://" + DYNAMODB_HOST
DYNAMODB_ORDERS_TABLE = config["dynamodb"]["orders_table"]

# Data Archiving Settings
CRYPTO_DATA_PATH: str = "Crypto/"
HISTORICAL_DATA_START: str = "2017-01-01 00:00:00"
USE_S3 = bool(strtobool(config["archive"]["use_s3"]))
S3_BUCKET = config["archive"]["s3_bucket"]

# Proxy Settings
# I use https://github.com/dan-v/awslambdaproxy, 3 instances
PROXY_USER: str = config["proxy"]["proxy_user"]
PROXY_PASS: str = config["proxy"]["proxy_pass"]

# Slack settings
SLACK_WORKSPACE: str = config["slack"]["slack_workspace"]
SLACK_WEBHOOK_URL: str = config["slack"]["slack_webhook_url"]
SLACK_CHANNEL: str = config["slack"]["slack_channel"].replace("#", "")
SLACK_TOKEN: str = config["slack"]["slack_token"]

# Machine Learning
ML_S3_BUCKET = config["ml"]["s3_bucket"]
ML_LOCAL_PATH = SYMPHONY_DIR + "ml/models/"
BACKTEST_S3_FOLDER = "backtest_results"



