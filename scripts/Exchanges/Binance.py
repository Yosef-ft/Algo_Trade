import os
import sys

from typing import *
import requests
import logging

sys.path.append(os.path.abspath('scripts'))
from logger import logging_setup


logger = logging_setup()


class BinanceClient:
    def __init__(self, futures= False):

        self.futures = futures

        if self.futures:
            self._base_url = 'https://testnet.binancefuture.com'     # old base endpoint: https://testnet.binancefuture.com
        else:
            self._base_url = 'https://testnet.binance.vision'    # old base endpoint: https://api.binance.com


        self.symbols = self.get_symbols()


    def _make_request(self, endpoint: str, query_parameters: Dict)-> Union[Dict, None]:
        '''
        This function is used to make requests

        Parameters:
        ----------
            endpoint(str): The endpoint you want to access
            query_parameters(Dict): A dictionary containing all your query sets
        '''

        try:
            response = requests.get(self._base_url + endpoint, params=query_parameters)
        except Exception as e: 
            response = None
            logger.error("Error while making request to %s: %s", endpoint, e)
        
        
        try:
            if response.status_code == 200:
                return response.json()
            
            else:
                logger.error("Error while making request to %s: %s (status code = %s)",
                            endpoint, response.json(), response.status_code)
                return None
        except: 
            logger.error("Error while making request to %s: %s", endpoint, e)


    def get_symbols(self, only_usdt: bool = True) -> list:
        '''
        This function gets all the symbols avalable in the binance exchange

        Parameter:
        ----------
            only_usdt(bool): if true: This allows as to return symbols with quote asset of USDT

        Returns:
        -------
            symbols(list): Returns a list of symbols
        '''

        params = dict()

        endpoint = '/fapi/v1/exchangeInfo' if self.futures else '/api/v1/exchangeInfo'
        
        data = self._make_request(endpoint, params)

        # To check if the response is valid or not
        if data != None:
            symbols_data = data['symbols']
            symbols = []
            
            # Only collect symbols that have a quote asset of USDT because they have a higher volume
            for symbol in symbols_data:
                if only_usdt:
                    if symbol['quoteAsset'] == 'USDT':
                        symbols.append(symbol['symbol'])

                else:
                    symbols.append(symbol['symbol'])
            
            return symbols
        
        return None
    

    def get_historica_data(self, symbol: str, interval: str = '1m', startTime: Optional[int] = None, endTime : Optional[int] = None) -> List:
        '''
        This function returns the Open time, open, high, low, close, volume for a given symbol

        Parameter:
        ----------
            symbol(str): The symobol we want the data
            interval(str): the interval of the data:
                ENUM: s-> seconds; m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
                        - 1s
                        - 1m, 3m, 5m, 15m, 30m
                        - 1h, 2h, 4h, 6h, 8h, 12h
                        - 1d, 3d, 1w
                        - 1M
            startTime (Optional[int]): The starting timestamp (Unix time) for the data retrieval.
            endTime (Optional[int]): The ending timestamp (Unix time) for the data retrieval.

        Returns:
        -------
            candlestick(List): Contains Open time, open, high, low, close prices and volume data for the given symbol and interval.                        
        '''

        params = dict()

        params['symbol'] = symbol
        params['interval'] = interval
        params['limit'] = 1500

        if startTime != None:
            params['startTime'] = startTime
        if endTime != None:
            params['endTime'] = endTime


        endpoint = '/fapi/v1/klines' if self.futures else '/api/v3/klines'   

        data = self._make_request(endpoint, query_parameters=params) 
        candles = []

        if data != None:
            for candle in data:
                candles.append((float(candle[0]), float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5])))
            logger.debug('Candles stick successfully scraped')
            return candles   

        else:
            return None


if __name__ == "__main__":
    bin = BinanceClient(True)
    candles = bin.get_historica_data('BTCUSDT')
    # print(candles)

