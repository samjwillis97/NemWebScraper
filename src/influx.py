from loguru import logger
import influxdb_client
import sys
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision


class InfluxDB:
    def __init__(self, url, token, org):
        self.url = url
        self.token = token
        self.org = org

        self.client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )
        self.write_api = self.client.write_api(
            write_options=SYNCHRONOUS,
        )

        logger.info("InfluxDB Initd")

    def write(self, bucket, org, data):
        if data is not None:
            try:
                self.write_api.write(
                    bucket,
                    org,
                    data,
                    # write_precision=Writ2ePrecision.S
                )
                logger.success("Data Sent to InfluxDB")
            except:
                logger.error("Error Sending Loads to InfluxDB")
                raise

    def close(self):
        self.client.close()