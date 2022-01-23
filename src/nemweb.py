import requests
import tempfile
import dbm
import os
import influxdb_client
import sqlite3
import pandas as pd

from bs4 import BeautifulSoup
from zipfile import ZipFile, BadZipFile
from loguru import logger

from load_env import DBM_STORE, DEBUG


def check_error(func):
    def wrapper(*args):
        if not args[0].error:
            return func(*args)
        else:
            return
    return wrapper


class NemWebPage:
    _base_url = "http://nemweb.com.au/"  # Reports/Current/Dispatch_SCADA/
    _temp_dir = tempfile.gettempdir()
    _temp_zip = _temp_dir + "/nemtemp.zip"
    _dbm_store = 'file_store'

    def __init__(self, url_ext):
        self.url_ext = url_ext

        self.zip_url = None
        self.zip_path = None
        self.csv_path = None
        self.csv_df = None
        self.csv_dict = None
        self.influx_points = None
        self.error = False

    # gets url of recent zip file from url
    @check_error
    def get_recent_zip_url(self):
        # Create URL and Download Page
        try:
            url = self._base_url + self.url_ext
            r = requests.get(url)

            # BS4 OBject
            source = BeautifulSoup(r.text, "html.parser")

            # Get Final A Element and Create a Download URL
            dl_url_ext = source.find_all('a')[-1].get('href')
            self.zip_url = self._base_url + dl_url_ext[1:]

            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - Recent Zip URL: {self.zip_url}")
        except IndexError:
            self.error = True
            logger.warning("Error parsing webpage")

    # if zip is not in DBM returns true
    @check_error
    def is_new_zip(self):
        with dbm.open(DBM_STORE, 'c') as db:
            try:
                if db[self.url_ext].decode('UTF-8') != self.zip_url:
                    db[self.url_ext] = self.zip_url
                    if DEBUG:
                        logger.debug(f"{self.__class__.__name__} - ZIP URL is New")
                    return True
                else:
                    logger.info(f"{self.__class__.__name__} - Load File Already Downloaded")
                    return False
            except:
                logger.info(f"{self.__class__.__name__} - Url does not exist in DBM")
                db[str(self.url_ext)] = self.zip_url
                return True

    # downloads zip if it is new
    @check_error
    def download_zip(self):
        if self.is_new_zip():
            r = requests.get(self.zip_url, stream=True)
            with open(self._temp_zip, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)
            self.zip_path = self._temp_zip

            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - Zip Downloaded")

    # unzips file at zip_path
    @check_error
    def unzip(self):
        if (self.zip_path is not None) and (os.path.exists(self.zip_path)):
            try:
                with ZipFile(self.zip_path, 'r') as zipObj:
                    zipObj.extractall(self._temp_dir)
                    self.csv_path = os.path.join(self._temp_dir, zipObj.namelist()[0])
                    if DEBUG:
                        logger.debug(f"{self.__class__.__name__} - CSV Located at {self.csv_path}")
            except BadZipFile:
                self.error = True
                logger.warning(f"Bad zip file.")
        else:
            self.error = True
            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - No File Path or No Zip Found")

    # Deletes files present at zip_path and csv_path
    @check_error
    def delete_files(self):
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - Deleting Files")
        if self.zip_path is not None:
            os.remove(self.zip_path)
        if self.csv_path is not None:
            os.remove(self.csv_path)

    @check_error
    def df_to_dict(self):
        if self.csv_df is not None:
            self.csv_dict = self.csv_df.to_dict('index')
            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - Converting DF to Dict")
                logger.debug(f"{self.__class__.__name__} - Printing Dict")
                logger.debug(self.csv_dict)

    @check_error
    def csv_to_df(self):
        pass

    @check_error
    def dict_to_influx(self):
        pass

    @check_error
    def download_and_process(self):
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - Starting Download and Process")

        self.get_recent_zip_url()
        self.download_zip()
        self.unzip()
        self.csv_to_df()
        self.df_to_dict()
        self.delete_files()
        self.dict_to_influx()


class NemWebLoads(NemWebPage):
    @check_error
    def csv_to_df(self):
        if self.csv_path is not None:
            df = pd.read_csv(
                self.csv_path,
                skiprows=[0, 1],
                usecols=[4, 5, 6],
                names=['Time', 'Unit', 'MW'],
                parse_dates=[0],
            )
            df.dropna()
            self.csv_df = df

    @check_error
    def dict_to_influx(self):
        if self.csv_dict is not None:
            batch = []
            for row in self.csv_dict.items():
                if isinstance(row[1]['Unit'], str):
                    batch.append(influxdb_client.Point(
                            "generation"
                        ).tag(
                            "unit", str(row[1]['Unit'])
                        ).field(
                            "MW", float(row[1]['MW'])
                        )
                    )
            self.influx_points = batch


class NemWebSolar(NemWebPage):
    @check_error
    def csv_to_df(self):
        if self.csv_path is not None:
            df = pd.read_csv(
                self.csv_path,
                skiprows=[0, 1],
                usecols=[4, 5, 6, 7],
                names=['Time', 'RegionID', 'Power', 'QI'],
                parse_dates=[0],
            )
            df.dropna()
            self.csv_df = df

    @check_error
    def dict_to_influx(self):
        if self.csv_dict is not None:
            batch = []
            for row in self.csv_dict.items():
                if isinstance(row[1]['RegionID'], str):
                    batch.append(influxdb_client.Point(
                            "rooftop"
                        ).tag(
                            "regionId", str(row[1]['RegionID'])
                        ).field(
                            "MW", float(row[1]['Power'])
                        )
                        # ).field(
                        #     "QI", float(row[1]['QI'])
                        # )
                    )
            self.influx_points = batch


class NemWebDemand(NemWebPage):
    @check_error
    def csv_to_df(self):
        if self.csv_path is not None:
            df = pd.read_csv(
                self.csv_path,
                skiprows=[0, 1],
                usecols=[4, 5, 6],
                names=['RegionID', 'Time', 'Demand'],
                parse_dates=[1],
            )
            df.dropna()
            self.csv_df = df

    @check_error
    def dict_to_influx(self):
        if self.csv_dict is not None:
            batch = []
            for row in self.csv_dict.items():
                if isinstance(row[1]['RegionID'], str):
                    batch.append(influxdb_client.Point(
                            "demand"
                        ).tag(
                            "regionId", str(row[1]['RegionID'])
                        ).field(
                            "MW", float(row[1]['Demand'])
                        )
                        # TODO: Work out why the fuck it doesn't work with time
                        # ).time(
                        #     row[1]['Time'].to_pydatetime()
                        # )
                    )
            self.influx_points = batch
