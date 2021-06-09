import requests
import tempfile
import os
import datetime
import influxdb_client
import dbm
import pandas as pd

from bs4 import BeautifulSoup
from zipfile import ZipFile
from loguru import logger
from pprint import pprint
from distutils.util import strtobool
from dotenv import load_dotenv
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

#### FUNCTIOnS
def is_docker():
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )

# Setup Influx Client
def get_influx_client():
    client = influxdb_client.InfluxDBClient(
        url = INFLUX_URL,
        token=INFLUX_TOKEN,
        org = INFLUX_ORG
    )
    
    logger.info("Connected to InfluxDB")

    return client

def get_influx_write_api(influx_client):
    write_api = influx_client.write_api(
        write_options=SYNCHRONOUS
    )

    return write_api

def get_influx_query_api(influx_client):
    query_api = influx_client.query_api()
    return query_api

# Download ZIP File from URL and Save in Temp
def get_downloaded_recent_nemweb_zip(url):
    # Required Variables
    download_url_head = "http://nemweb.com.au/"
    temp_path = TEMP_DIR + "/nemtemp.zip"

    ## Request URL
    r = requests.get(url)

    ## Data
    data = r.text

    ## Make Data a BS4 Object
    source = BeautifulSoup(data, "html.parser")

    # Get Last A element and create Download URL
    a_element = source.find_all('a')[-1]
    url_ext = a_element.get('href')
    download_url = download_url_head + url_ext[1:]

    with dbm.open(DBM_STORE, 'c') as db:
        try:
            if db[url].decode('UTF-8') != download_url:
                db[url] = download_url
            else:
                logger.info("Load File Already Downloaded")
                return None
        except:
            logger.error("DBM Error")
            db[url] = ""

    # Download Zip File
    r = requests.get(download_url, stream=True)
    with open(temp_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size = 128):
            fd.write(chunk)
    
    if (DEBUG):
        logger.debug("Zip Downloaded")

    return temp_path

# Unzip and Format Data
def get_formatted_nemweb_load_zip(zip_path):
    # Open Downloaded Zip, extract to a CSV
    if zip_path is not None:
        with ZipFile(zip_path, 'r') as zipObj:
            zipObj.extractall(TEMP_DIR)
            csv_file = os.path.join(TEMP_DIR, zipObj.namelist()[0])

        # Open CSV to Pandas Dataframe
        df = pd.read_csv(
            zip_path,
            skiprows=[0,1],
            usecols=[4,5,6],
            names=['Time', 'Unit', 'MW'],
            parse_dates=[0],
        )

        # Drop bad Rows
        df.dropna()

        # Get time of file
        time = df.iloc[0]['Time']
        logger.info(f"Unit Loads for {time} Received")
        # s_time = round(time.replace(tzinfo=datetime.timezone.utc).timestamp())

        # Create Dict of Datapoints
        unit_loads = {'Time': time}
        for row in df.iterrows():
            unit_loads[row[1]['Unit']] = row[1]['MW']

        os.remove(csv_file)
        os.remove(zip_path)

        if (DEBUG):
            logger.debug("Unit Loads Extracted")
            logger.debug("Files Deleted")
            logger.debug("Printing DF")
            pprint(df)

        return unit_loads
    else:
        return None

def get_formatted_nemweb_price_zip(zip_path):
    if zip_path is not None:
        # Open Downloaded Zip, extract to a CSV
        with ZipFile(zip_path, 'r') as zipObj:
            zipObj.extractall(TEMP_DIR)
            csv_file = os.path.join(TEMP_DIR, zipObj.namelist()[0])

        # Open CSV to Pandas Dataframe
        df_price = pd.read_csv(
            zip_path,
            skiprows=[0,1,2,3,4,5,6,7,8],
            usecols=[4,6,8],
            names=['Time', 'Region', 'Price'],
            parse_dates=[0],
            nrows=5
        )
        df_price.dropna()

        df_dl = pd.read_csv(
            zip_path,
            skiprows=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],
            usecols=[4,6,8,9,10],
            names=['Time', 'Region', 'TotalDemand', 'TotalGen', 'AvailGen'],
            parse_dates=[0],
            nrows=5
        )
        df_dl.dropna()

        # Get time of file
        time = df_price.iloc[0]['Time']
        logger.info(f"Pricing and Demand for {time} Received")

        # Create Dict of Datapoints
        pricing_demand = {'Time': time}
        for row in df_price.iterrows():
            pricing_demand[row[1]['Region']] = {}
            pricing_demand[row[1]['Region']]['Price'] = row[1]['Price']
        
        for row in df_dl.iterrows():
            pricing_demand[row[1]['Region']]['TotalDemand'] = row[1]['TotalDemand']
            pricing_demand[row[1]['Region']]['TotalGen'] = row[1]['TotalGen']
            pricing_demand[row[1]['Region']]['AvailGen'] = row[1]['AvailGen']

        os.remove(csv_file)
        os.remove(zip_path)

        if (DEBUG):
            logger.debug("Pricing Extracted")
            logger.debug("Files Deleted")
            logger.debug("Printing Price DF")
            pprint(df_price)
            logger.debug("Printing Demand DF")
            pprint(df_dl)

        return pricing_demand
    else:
        return None

# Send Data to Influx
def send_nemweb_load_to_influxdb(influx_write_api, unit_loads):
    ## Creating Line Protocol Query for Influx
    if unit_loads != None:
        batch = []
        for unit in unit_loads.items():
            if (isinstance(unit[0], str) and unit[0] != "Time"):
                batch.append(
                    influxdb_client.Point(
                            "load"
                        ).tag(
                            "unit", str(unit[0])
                        ).field(
                            "MW", unit[1]
                        # ).time(2
                        #     s_time
                        )
                    )

        # logger.debug(batch)

        ## Sending to Influx DB
        try:
            influx_write_api.write(
                INFLUX_BUCKET,
                INFLUX_ORG,
                batch,
                # write_precision=Writ2ePrecision.S
            )
            logger.success("Loads Sent to InfluxDB")
        except:
            logger.error("Error Sending Loads to InfluxDB")

def send_nemweb_pl_to_influxdb(influx_write_api, pricing):
    if pricing is not None:
        batch = []
        for region in pricing.items():
            if (isinstance(region[0], str) and region[0] != "Time"):
                for key, value in region[1].items():
                    if key == 'Price':
                        unit = '$/MWh'
                    else:
                        unit = 'MW'
                    
                    batch.append(
                        influxdb_client.Point(
                            region[0]
                        ).tag(
                            "unit", unit
                        ).field(
                            key, value
                        )
                    )
        try:
            influx_write_api.write(
                INFLUX_BUCKET,
                INFLUX_ORG,
                batch,
                # write_precision=Writ2ePrecision.S
            )
            logger.success("Pricing Sent to InfluxDB")
        except:
            logger.error("Error Sending Pricing to InfluxDB")

#### MAIN
if __name__ == "__main__":

    #### SETTING VARIABLES
    logger.info("Scraper Start")

    ## Import Env Values from OS or .env File if not in docker
    if (not is_docker()):
        logger.info("Loading .env File")
        load_dotenv()

    ## Read Env Values
    DEBUG = bool(strtobool(os.getenv("DEBUG", False)))
    CLEAR_DBM_ON_START = bool(strtobool(os.getenv("CLEAR_DBM_ON_START", False)))
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "default_bucket")
    INFLUX_ORG = os.getenv("INFLUX_ORG", "default_org")
    INFLUX_TOKEN = os.getenv("INFLUX_TOKEN","default_token")
    INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")

    if (DEBUG):
        logger.info("DEBUG Mode Started")

    ## Init Temp Directory
    TEMP_DIR = tempfile.gettempdir()

    ##
    DBM_STORE = 'file_store'
    if (CLEAR_DBM_ON_START):
        logger.info("Clearing DBM")
        with dbm.open(DBM_STORE, 'n') as db:
            pass

    ## INFLUXDB
    influx_client = get_influx_client()
    influx_write_api = get_influx_write_api(influx_client)
    influx_query_api = get_influx_query_api(influx_client)

    ## NEMWEB LOADS
    nemweb_load_zip = get_downloaded_recent_nemweb_zip(
        "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/")
    unit_loads = get_formatted_nemweb_load_zip(nemweb_load_zip)
    send_nemweb_load_to_influxdb(influx_write_api, unit_loads)

    ## NEMWEB PRICING + GENERATION
    nemweb_price_zip = get_downloaded_recent_nemweb_zip(
        "http://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/")
    pricing = get_formatted_nemweb_price_zip(nemweb_price_zip)
    send_nemweb_pl_to_influxdb(influx_write_api, pricing)

    ## Close InfluxDB Connections
    influx_client.close()

    logger.info("Scraper Ended")