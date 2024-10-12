import os
import sys
import datetime
import logging
import pandas as pd

TF_EQUIV = {'1m' : '1Min', '5m': '5Min', '15m': '15Min', '30m':'30Min', '1h': '1H',
            '4h': '4H', '6h':'6H', '12h':'12H', '1d' : 'D'}

class Util:

    def ms_to_dt(self, ms: int) ->datetime.datetime:
        '''
        Function to change milli seconds to datetime
        '''
        return datetime.datetime.fromtimestamp(ms / 1000)


    def resample_timeframe(self, data: pd.DataFrame, tf: str)-> pd.DataFrame:
        '''
        This funtion is used to resample the data from dataframe 1 minutes to different timeframes
        '''
        return data.resample(TF_EQUIV[tf]).agg(
                {'Open' : 'first', 'High' : 'max', 'Low' : 'min', 'Close':'last', 'Volume': 'sum'}
            )



from data_collector import *
from Exchanges.Binance import BinanceClient

if __name__ == "__main__":
    bin = BinanceClient(False)
    print(bin.get_historica_data('BTCUSDT'))
