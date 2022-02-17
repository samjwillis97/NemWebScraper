## LOAD ENV VARIABLES FOR MAIN AND NEMWEB
import os
import dbm
from dotenv import load_dotenv
from loguru import logger
from distutils.util import strtobool


def is_docker():
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )


if not is_docker():
    logger.info("Loading .env File")
    load_dotenv()

# Read Env Values
DEBUG = bool(strtobool(os.getenv("DEBUG", False)))
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "default_bucket")
INFLUX_ORG = os.getenv("INFLUX_ORG", "default_org")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "default_token")
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")

if DEBUG:
    logger.info("DEBUG Mode Started")

DBM_STORE = '/data/dbm_store'
