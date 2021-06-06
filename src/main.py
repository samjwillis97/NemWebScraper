import requests
import tempfile
import os
import datetime
import influxdb_client
import pandas as pd

from bs4 import BeautifulSoup
from zipfile import ZipFile
from loguru import logger
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

logger.add("file_{time}.log", retention="2 days")

logger.info("Scraper Start")

## Import Env Values from OS
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "default_bucket")
INFLUX_ORG = os.getenv("INFLUX_ORG", "default_org")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN","default_token")
INFLUX_URL = os.getenv("INFLUX_URL", "localhost:8086")

# Setup Influx Client
client = influxdb_client.InfluxDBClient(
    url = INFLUX_URL,
    token=INFLUX_TOKEN,
    org = INFLUX_ORG
)
write_api = client.write_api(
    write_options=SYNCHRONOUS
)

logger.info("Connected to InfluxDB")

## Init Temp Directory
temp_dir = tempfile.gettempdir()
temp_path = temp_dir + "/nemtemp.zip"

## URL FOR SITE TO SCRAPE
url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
download_url_head = "http://nemweb.com.au/"

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

# Download Zip File
r = requests.get(download_url, stream=True)
with open(temp_path, 'wb') as fd:
    for chunk in r.iter_content(chunk_size = 128):
        fd.write(chunk)

logger.info("Zip Downloaded")

# Open Downloaded Zip, extract to a CSV
with ZipFile(temp_path, 'r') as zipObj:
    zipObj.extractall(temp_dir)
    csv_file = os.path.join(temp_dir, zipObj.namelist()[0])

# Open CSV to Pandas Dataframe
df = pd.read_csv(
    temp_path,
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
s_time = round(time.replace(tzinfo=datetime.timezone.utc).timestamp())

# Create Dict of Datapoints
unit_loads = {}
for row in df.iterrows():
    unit_loads[row[1]['Unit']] = row[1]['MW']

logger.info("Data Extracted")

## Creating Line Protocol Query for Influx
batch = []
for unit in unit_loads.items():
    if isinstance(unit[0], str):
        batch.append(
            influxdb_client.Point(
                    "load"
                ).tag(
                    "unit", str(unit[0])
                ).field(
                    "MW", unit[1]
                # ).time(
                #     s_time
                )
            )

# logger.debug(batch)

## Sending to Influx DB
try:
    write_api.write(
        INFLUX_BUCKET,
        INFLUX_ORG,
        batch,
        # write_precision=Writ2ePrecision.S
    )
    logger.success("Sent to InfluxDB")
except:
    logger.error("Error Sending to InfluxDB")

## Close InfluxDB Connections
client.close()

logger.info("Deleting Files")
os.remove(temp_path)
os.remove(csv_file)