import time
from typing import *

from Utils import Util
from Exchanges.Binance import BinacneClient

utils = Util()
logger = utils.logging_setup()

def collect_all(client: BinacneClient, symbol: str):

    oldest_ts, recent_ts = None, None

    # Initial Request

    if oldest_ts == None:
        data = client.get_historica_data(symbol, endTime=int(time.time() * 1000) -60000) 

        if len(data) == 0:
            logger.warning("%s: no initial data found", symbol)
            return
        
        else:
            logger.info("%s: collected %s initial data from %s to %s", symbol, len(data),
                        utils.ms_to_dt(data[0][0]), utils.ms_to_dt(data[-1][0]))
            
        oldest_ts = data[0][0]
        recent_ts = data[-1][0]

    # Most recent data
    
    while True:
        data = client.get_historica_data(symbol, startTime=int(oldest_ts - 60000))

        if data is None:
            time.sleep(5)
            continue

        data = data[:-1]

        if len(data) <=2:
            break


        if data[-1][0] > recent_ts:
            recent_ts = data[-1][0]


        logger.info("%s: Collected %s recent data from %s to %s", symbol, len(data),
            utils.ms_to_dt(data[0][-1]), utils.ms_to_dt(data[-1][0]))

       
        time.sleep(1.1)

    # Older data

    while True:
        data = client.get_historica_data(symbol, endTime=int(oldest_ts - 60000))

        if data is None:
            time.sleep(5)
            continue


        if len(data) <=2:
            logger.info("%s: Stopped older data collection because no data was found before %s", symbol,
                        utils.ms_to_dt(oldest_ts))
            break


        if data[0][0] > oldest_ts:
            oldest_ts = data[0][0]


        logger.info("%s: Collected %s older data from %s to %s", symbol, len(data),
            utils.ms_to_dt(data[0][-1]), utils.ms_to_dt(data[-1][0]))

       
        time.sleep(1.1)


if __name__ == '__main__':
    bin = BinacneClient()
    collect_all(bin, 'BTCUSDT')