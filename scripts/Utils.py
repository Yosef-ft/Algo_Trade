import os
import sys
import datetime
import logging
import pandas as pd

TF_EQUIV = {'1m' : '1Min', '5m': '5Min', '15m': '15Min', '30m':'30Min', '1h': '1H',
            '4h': '4H', '6h':'6H', '12h':'12H', '1d' : 'D'}

class Util:

    def logging_setup(self):
        '''
        This funciton is used to setup logging
        '''    
        log_dir = os.path.join(os.getcwd(), 'logs')

        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        log_file_info = os.path.join(log_dir, 'Info.log')
        log_file_error = os.path.join(log_dir, 'Error.log')

        formatter = logging.Formatter('%(asctime)s - %(levelname)s :: %(message)s',
                                    datefmt='%Y-%m-%d %H:%M')

        info_handler = logging.FileHandler(log_file_info)
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)

        error_handler = logging.FileHandler(log_file_error)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)


        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(info_handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)       

        return logger 
    

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
