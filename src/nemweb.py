import datetime

import requests
import tempfile
import dbm
import os
import influxdb_client
import pandas as pd

from bs4 import BeautifulSoup
from zipfile import ZipFile, BadZipFile
from loguru import logger
from influxdb_client import WritePrecision
from pprint import pprint

from load_env import DBM_STORE, DEBUG


def check_error(func):
    def wrapper(*args):
        if not args[0].error:
            return func(*args)
        else:
            return
    return wrapper


class NemWebPage:
    _base_url = "http://nemweb.com.au/" # Reports/Current/Dispatch_SCADA/
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
    def GetRecentZipUrl(self):
        ## Create URL and Download Page
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
    def IsNewZip(self):
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
    def DownloadZip(self):
        if self.IsNewZip():
            r = requests.get(self.zip_url, stream=True)
            with open(self._temp_zip, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)
            self.zip_path = self._temp_zip

            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - Zip Downloaded")

    # unzips file at zip_path
    @check_error
    def Unzip(self):
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
    def DeleteFiles(self):
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - Deleting Files")
        if self.zip_path is not None:
            os.remove(self.zip_path)
        if self.csv_path is not None:
            os.remove(self.csv_path)


    @check_error
    def DFtoDict(self):
        if self.csv_df is not None:
            self.csv_dict = self.csv_df.to_dict('index')
            if DEBUG:
                logger.debug(f"{self.__class__.__name__} - Converting DF to Dict")
                logger.debug(f"{self.__class__.__name__} - Printing Dict")
                logger.debug(self.csv_dict)

    @check_error
    def CSVtoDF(self):
        pass

    @check_error
    def DictToInflux(self):
        pass

    @check_error
    def DownloadAndProcess(self):
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - Starting Download and Process")

        self.GetRecentZipUrl()
        self.DownloadZip()
        self.Unzip()
        self.CSVtoDF()
        self.DFtoDict()
        self.DeleteFiles()
        self.DictToInflux()


class NemWebLoads(NemWebPage):
    @check_error
    def CSVtoDF(self):
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
    def DictToInflux(self):
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
    def CSVtoDF(self):
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
    def DictToInflux(self):
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
    def CSVtoDF(self):
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
    def DictToInflux(self):
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

# class NemWebPriceAndGen(NemWebPage):
#     @check_error
#     def CSVtoDF(self):
#         if self.csv_path is not None:
#             if DEBUG:
#                 logger.debug(f"{self.__class__.__name__} - Converting CSV to DF")
#
#             df_price = pd.read_csv(
#                 self.csv_path,
#                 skiprows=[0,1,2,3,4,5,6,7,8],
#                 usecols=[4,6,8],
#                 names=['Time', 'Region', 'Price'],
#                 parse_dates=[0],
#                 nrows=5
#             )
#             df_price.dropna()
#             df_price.sort_values(by='Region', inplace=True)
#             price_dict = df_price.to_dict('index')
#
#             df_gen = pd.read_csv(
#                 self.csv_path,
#                 skiprows=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],
#                 usecols=[4,6,8,9,10],
#                 names=['Time', 'Region', 'TotalDemand', 'TotalGen', 'AvailGen'],
#                 parse_dates=[0],
#                 nrows=5
#             )
#
#             df_gen.dropna()
#             df_gen.sort_values(by='Region', inplace=True)
#             gen_dict = df_gen.to_dict('index')
#
#             for index, obj in gen_dict.items():
#                 gen_dict[index] = {**obj,  **price_dict[index]}
#
#             self.csv_df = df_price.from_dict(gen_dict, orient='index')
#
#             if DEBUG:
#                 logger.debug(f"{self.__class__.__name__} - Printing DF")
#                 logger.debug(self.csv_df)
#
#     @check_error
#     def DictToInflux(self):
#         if self.csv_dict is not None:
#             if DEBUG:
#                 logger.debug(f"{self.__class__.__name__} - Converting Dict to Influx Points")
#
#             batch = []
#             for region in self.csv_dict.items():
#                 if (isinstance(region[1]['Region'], str)):
#                     for key, value in region[1].items():
#                         if key != 'Time' and key != 'Region':
#
#                             if key == 'Price':
#                                 unit = "$/MWh"
#                                 meas = "price"
#                             else:
#                                 meas = "generation"
#                                 unit ="MW"
#
#                             batch.append(influxdb_client.Point(
#                                     meas
#                                 ).tag(
#                                     "state", region[1]['Region']
#                                 ).tag(
#                                     "unit", unit
#                                 ).field(
#                                     key, float(value)
#                                 )
#                             )
#             self.influx_points = batch
