import time
from typing import *
import logging

from Exchanges.Binance import BinanceClient
from databases import Database
from Utils import Util

utils = Util()
logger = utils.logging_setup()



def collect_all(client: BinanceClient, symbol: str, exchange: str):

    '''
    This function collects data from exchanges and saves it to databases

    Parameter:
    ----------
        client(BinanceClient): Which is the class the has the get_historical_data method
        symbol(str): 
        exchange(str): The name of exchange. eg. Binance
    '''

    h5_db = Database(exchange)
    h5_db.create_dataset(symbol)

    oldest_ts, recent_ts = h5_db.get_first_last_timestamp(symbol)
    

    # Initial Request

    if oldest_ts == None:
        data = client.get_historica_data(symbol, endTime=int((time.time() * 1000) - 60000))

        if len(data) == 0:
            logger.warning("%s: no initial data found", symbol)
            return
        
        else:
            logger.info("%s: collected %s initial data from %s to %s", symbol, len(data),
                        utils.ms_to_dt(data[0][0]), utils.ms_to_dt(data[-1][0]))
            
        oldest_ts = data[0][0]
        recent_ts = data[-1][0]
        
        h5_db.write_data(symbol, data)

    data_to_insert = []

    # Most recent data
    
    while True:
        data = client.get_historica_data(symbol, startTime= int(recent_ts + 60000))

        if data is None:
            time.sleep(5)
            continue

        data = data[:-1]
        data_to_insert = data_to_insert + data

        if len(data_to_insert) > 10000:
            h5_db.write_data(symbol, data_to_insert)
            data_to_insert.clear()

        if len(data) <=2:
            break


        if data[-1][0] > recent_ts:
            recent_ts = data[-1][0]


        logger.info("%s: Collected %s recent data from %s to %s", symbol, len(data),
            utils.ms_to_dt(data[0][0]), utils.ms_to_dt(data[-1][0]))

        time.sleep(1.1)

    h5_db.write_data(symbol, data_to_insert)
    data_to_insert.clear()              

    # Older data

    while True:
        data = client.get_historica_data(symbol, endTime= int(oldest_ts - 60000))


        if data is None:
            time.sleep(5)
            continue


        if len(data) ==0:
            logger.info("%s: Stopped older data collection because no data was found before %s", symbol,
                        utils.ms_to_dt(oldest_ts))
            break

        if len(data_to_insert) > 10000:
            h5_db.write_data(symbol, data_to_insert)
            data_to_insert.clear()        

        if data[0][0] < oldest_ts:
            oldest_ts = data[0][0]


        logger.info("%s: Collected %s older data from %s to %s", symbol, len(data),
            utils.ms_to_dt(data[0][0]), utils.ms_to_dt(data[-1][0]))

        time.sleep(1.1)

    h5_db.write_data(symbol, data_to_insert)
    data_to_insert.clear()    

if __name__ == '__main__':
    bin = BinanceClient(True)
    collect_all(bin, 'BTCUSDT', 'Binance')