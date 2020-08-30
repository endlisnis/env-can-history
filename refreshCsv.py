#!/usr/bin/env python3.8

import csv
import dataclasses
import datetime as dt
import lzma
import os
import requests
import sqlitedict
import threading
import time as timelib

from concurrent.futures import ThreadPoolExecutor


@dataclasses.dataclass
class InventoryStation:
    name: str
    province: str
    climateId: str
    stationId: int
    wmoId: int
    tcId: str
    latitudeDecimalDegrees: float
    longitudeDecimalDegrees: float
    latitude: int
    longitude: int
    elevation: float
    firstYear: int
    lastYear: int
    hlyFirstYear: int
    hlyLastYear: int
    dlyFirstYear: int
    dlyLastYear: int
    mlyFirstYear: int
    mlyLastYear: int

pool = ThreadPoolExecutor(max_workers=8)
futures = []

class LocalSession(threading.local):
    def __init__(self):
        super().__init__()
        self.session = requests.Session()

threadLocal = LocalSession()
stationRefresh = sqlitedict.SqliteDict('StationRefresh.db', autocommit=True)

def getOneFile(url, dirname, localPath):
    print(url)
    os.makedirs(dirname, exist_ok=True)
    response = threadLocal.session.get(url, timeout=10)
    f = lzma.open(localPath, 'wb')
    f.write(response.content)
    f.close()
    stationRefresh[localPath] = timelib.time()
    # print('done')


def calcRefresh(year, lastRefresh):
    today = dt.date.today()
    threeDaysAgo = today - dt.timedelta(days=3)
    if year == today.year or year == threeDaysAgo.year:
        if timelib.time() - lastRefresh > 3600:
            # It's the last 3 days, refresh once per hour
            return True
    elif year == today.year - 1:
        if timelib.time() - lastRefresh > 3600*24*30:
            # It's last year, refresh once per month
            return True
    elif timelib.time() - lastRefresh > 3600*24*365:
        # Refresh at least once per year
        return True
    return False

def main():
    csvData = ( open('Station Inventory EN.csv')
                .read()
                .split('\n') )
    while not csvData[0].startswith('"Name"'):
        csvData.pop(0)

    for rowIndex, tokens in enumerate(csv.reader(csvData)):
        if rowIndex == 0:
            expectedHeader = [
                "Name", "Province", "Climate ID", "Station ID", "WMO ID", "TC ID",
                "Latitude (Decimal Degrees)", "Longitude (Decimal Degrees)", "Latitude",
                "Longitude", "Elevation (m)", "First Year", "Last Year", "HLY First Year",
                "HLY Last Year", "DLY First Year", "DLY Last Year", "MLY First Year", "MLY Last Year"]
            assert tokens == expectedHeader
            continue
        if len(tokens) == 0:
            continue
        fields = dataclasses.fields(InventoryStation)
        for i, field in enumerate(fields):
            if len(tokens[i]) == 0:
                tokens[i] = None
            else:
                tokens[i] = field.type(tokens[i])
        station = InventoryStation(*tokens)
        if station.dlyLastYear == 2020:
            dirname = f'stations/{station.stationId//1000}/{station.stationId}'
            # print(f'{station.name.title()}: {dirname}: {station.dlyFirstYear}-{station.dlyLastYear}')
            for year in range(station.dlyFirstYear, station.dlyLastYear+1):
                fname = f'{dirname}/{year}.csv.xz'
                refresh = False
                lastRefresh = stationRefresh.get(fname, 0)
                if calcRefresh(year, lastRefresh) is False:
                    continue
                url = ( f'http://climate.weather.gc.ca/climate_data/bulk_data_e.html'
                        f'?format=csv&stationID={station.stationId}&Year={year}&Month=1&Day=1'
                        f'&timeframe=2' )
                futures.append(pool.submit(getOneFile, url, dirname, fname))
    while len(futures):
        futures.pop(0).result()


if __name__=='__main__':
    main()
