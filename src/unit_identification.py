import logging
import os
import tempfile
import requests
import sqlite3
import pandas as pd
from sqlite3 import Error
from loguru import logger

from load_env import DEBUG


def check_error(func):
    def wrapper(*args):
        if not args[0].error:
            return func(*args)
        else:
            return
    return wrapper


class UnitID:
    def __init__(self):
        self.error = False
        self._df = None
        self._url = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration" \
                   "-and-Exemption-List.xls "
        self._xls_file = tempfile.gettempdir() + "/nem_unit.xls"
        self._conn = self._connect_to_db()
        self._cursor = self._conn.cursor()

    @check_error
    def _connect_to_db(self):
        conn = sqlite3.connect("/data/database.sqlite")
        logger.info("Connected to DB successfully")
        return conn

    @check_error
    def _create_tables(self):
        unit_table_create_query = """
            CREATE TABLE IF NOT EXISTS units (
                id int PRIMARY KEY,
                duid string UNIQUE NOT NULL,
                station_name string NOT NULL,
                region_id string NOT NULL,
                fuel_source string NOT NULL,
                technology_type string NOT NULL,
                max_capacity real 
            )
        """
        try:
            c = self._conn.cursor()
            c.execute(unit_table_create_query)
        except Error as e:
            logger.warning(f"{self.__class__.__name__} - Error Creating Tables: {e}")

    @check_error
    def _download_xls(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, '
                                 'like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        r = requests.get(self._url, stream=True, headers=headers)
        with open(self._xls_file, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - XLS Downloaded")

    @check_error
    def _process_xls(self):
        self._df = pd.read_excel(
            self._xls_file,
            sheet_name=3,
            engine="xlrd",
        )
        if DEBUG:
            logger.debug(f"{self.__class__.__name__} - {self._df}")

    @check_error
    def _write_to_db(self):
        for index, row in self._df.iterrows():
            if row["Dispatch Type"] == "Generator" and row["DUID"] != "-":
                select_query = f"""
                    SELECT id FROM units WHERE duid="{row["DUID"]}"
                """
                rows = self._cursor.execute(select_query)
                row = self._sanitize_row(row)
                if len(rows.fetchall()) == 0:
                    self._insert_db_row(row)
                else:
                    self._update_db_row(row)

    @check_error
    def _delete_xls(self):
        os.remove(self._xls_file)

    @check_error
    def _sanitize_row(self, row):
        capacity = row["Max Cap (MW)"]
        if row["Max Cap (MW)"] == "-":
            capacity = 0
        row["Max Cap (MW)"] = capacity

        if isinstance(row["Station Name"], str):
            row["Station Name"] = row["Station Name"].replace('"', "").strip()
        if isinstance(row["DUID"], str):
            row["DUID"] = row["DUID"].strip()
        if isinstance(row["Region"], str):
            row["Region"] = row["Region"].strip().upper()
        if isinstance(row["Fuel Source - Descriptor"], str):
            row["Fuel Source - Descriptor"] = row["Fuel Source - Descriptor"].strip()
        if isinstance(row["Technology Type - Primary"], str):
            row["Technology Type - Primary"] = row["Technology Type - Primary"].strip()

        return row

    @check_error
    def _insert_db_row(self, row):
        insert_query = f"""
            INSERT INTO units (
                duid,
                station_name,
                region_id,
                fuel_source,
                technology_type,
                max_capacity
            ) VALUES (
                "{row["DUID"]}",
                "{row["Station Name"]}",
                "{row["Region"]}",
                "{row["Fuel Source - Descriptor"]}",
                "{row["Technology Type - Primary"]}",
                {row["Max Cap (MW)"]}
            )
        """
        logger.debug(insert_query)
        self._cursor.executescript(insert_query)
        self._conn.commit()

    @check_error
    def _update_db_row(self, row):
        update_query = f"""
            UPDATE OR ROLLBACK units
            SET (
                station_name,
                region_id,
                fuel_source,
                technology_type,
                max_capacity
            ) = (
                "{row["Station Name"]}",
                "{row["Region"]}",
                "{row["Fuel Source - Descriptor"]}",
                "{row["Technology Type - Primary"]}",
                {row["Max Cap (MW)"]}
            )
            WHERE duid="{row["DUID"]}"
        """
        logger.debug(update_query)
        self._cursor.executescript(update_query)
        self._conn.commit()

    # SELECT DISTINCT region_id
    # FROM units

    def update(self):
        self._create_tables()
        self._download_xls()
        self._process_xls()
        self._write_to_db()
        self._delete_xls()
