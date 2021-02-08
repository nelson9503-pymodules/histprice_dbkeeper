from . import mysqlite
from datetime import datetime
import random


class DBKeeper:

    def __init__(self, folder_path: str):
        """
        Histrical Price Database Keeper manage the histical price data.
        Keeper will save the .db in the folder_path.
        """
        self.db_path = folder_path + "/histrical_price.db"
        self.__initialize()

    def update(self, symbol: str, data: dict, skipUpdated: bool = True):
        """
        Update the historical price data to database.
        If skipUpdated is True, keeper will skip the symbol had been updated today.
        """
        # create table if not exists
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                self.__create_price_table(symbol)
        self.tb = self.db.TB(symbol)
        # skip if updated
        today = self.__get_today()
        self.config = self.master.query(
            "*", 'WHERE table_name = "{}"'.format(symbol))
        lastupdate = self.config[symbol]["last_update"]
        if skipUpdated == True and today == lastupdate:
            return
        # random check
        check = self.__random_check_data_points(data)
        if check == False:
            self.__resync_database_to_data(symbol, data)
        # insert new data
        updates = {}
        last_date = self.config[symbol]["last_date"]
        for date in data:
            if date > last_date:
                updates[date] = data[date]
        self.tb.update(updates)
        # update lastupdate
        self.config[symbol]["last_update"] = today
        if self.config[symbol]["first_date"] == 0 and len(data) > 0:
            self.config[symbol]["first_date"] = min(list(data.keys()))
        if len(data) > 0:
            self.config[symbol]["last_date"] = max(list(data.keys()))
        if self.config[symbol]["data_points"] == 0:
            self.config[symbol]["data_points"] = len(data)
        else:
            self.config[symbol]["data_points"] += len(updates)
        self.master.update(self.config)
        self.db.commit()

    def query_price(self, symbol: str, start_time_stamp: int = None, end_time_stamp: int = None) -> dict:
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                return False
        self.tb = self.db.TB(symbol)
        condition = ""
        if not start_time_stamp == None:
            condition += "date >= {}".format(start_time_stamp)
        if not end_time_stamp == None:
            if len(condition) > 0:
                condition += " AND "
            condition += "date <= {}".format(end_time_stamp)
        if len(condition) > 0:
            condition = "WHERE " + condition
        query = self.tb.query("*", condition)
        return query

    def query_master_info(self, symbol: str) -> dict:
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                return False
        info = self.mastertb[symbol]
        return info

    def query_full_master_info(self) -> dict:
        self.mastertb = self.master.query()
        return self.mastertb

    def __initialize(self):
        self.db = mysqlite.DB(self.db_path)
        if not "master" in self.db.listTB():
            self.master = self.db.createTB("master", "table_name", "CHAR(100)")
            self.master.addCol("last_update", "INT")
            self.master.addCol("first_date", "INT")
            self.master.addCol("last_date", "INT")
            self.master.addCol("data_points", "INT")
        else:
            self.master = self.db.TB("master")
        self.mastertb = self.master.query()

    def __create_price_table(self, symbol: str):
        self.tb = self.db.createTB(symbol, "date", "INT")
        self.tb.addCol("open", "FLOAT")
        self.tb.addCol("high", "FLOAT")
        self.tb.addCol("low", "FLOAT")
        self.tb.addCol("close", "FLOAT")
        self.tb.addCol("adjclose", "FLOAT")
        self.tb.addCol("volume", "BIGINT")
        self.master.update({
            symbol: {
                "last_update": 0,
                "first_date": 0,
                "last_date": 0,
                "data_points": 0
            }})
        self.db.commit()

    def __get_today(self) -> int:
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        return int(now.timestamp())

    def __random_check_data_points(self, data: dict) -> bool:
        """
        Check adjust close of 20 data points randomly to ensure that
        the adjust close did not be recalucated.
        """
        dates = list(data.keys())
        random.shuffle(dates)
        if len(dates) >= 20:
            samples = dates[:20]
        else:
            samples = dates
        for date in samples:
            query = self.tb.query(
                "*", "WHERE date == {}".format(date))
            if len(query) == 0:
                continue
            if not round(query[date]["adjclose"], 4) == round(data[date]["adjclose"], 4):
                return False
        return True

    def __resync_database_to_data(self, symbol: str, data: dict):
        """
        If the random check failed,
            1. remove all dates that exist in database but not exists in input data;
            2. sync the data values for dates that both exist in database and input data;
        """
        dbdata = self.tb.query("date")
        if len(data) > 0:
            mindate = min(list(data.keys()))
        else:
            mindate = 0

        # remove dates that smaller than the first date from input data
        del_dates = []
        for date in dbdata:
            if date < mindate:
                self.tb.dropData(date)
                del_dates.append(date)
            else:
                break
        for date in del_dates:
            del dbdata[date]

        # refresh the data from input data
        updates = {}
        for date in dbdata:
            if date in data:
                updates[date] = data[date]
            else:
                self.tb.dropData(date)

        # config
        if len(dbdata) > 0:
            self.config[symbol]["first_date"] = min(list(dbdata.keys()))
        if len(dbdata) > 0:
            self.config[symbol]["last_date"] = max(list(dbdata.keys()))
        dates = self.tb.query("date")
        self.config[symbol]["data_points"] = len(dates)

        self.tb.update(updates)
