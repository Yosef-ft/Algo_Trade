from typing import *
import requests
import logging


logger = logging.getLogger()


class BinacneClient:
    def __init__(self, futures= False):

        self.futures = futures

        if self.futures:
            self._base_url = 'https://testnet.binancefuture.com'     # old base endpoint: https://testnet.binancefuture.com
        else:
            self._base_url = 'https://testnet.binance.vision'    # old base endpoint: https://api.binance.com


        self.symbols = self.get_symbols()


    def _make_request(self, endpoint: str, query_parameters: Dict):

        try:
            response = requests.get(self._base_url + endpoint, params=query_parameters)
        except Exception as e: 
            logger.error("Error while making request to %s: %s", endpoint, e)

        if response.status_code == 200:
            return response.json()
        
        else:
            logger.error("Error while making request to %s: %s (status code = %s)",
                         endpoint, response.json(), response.status_code)
            return None


    def get_symbols(self, only_usdt: bool = True) -> list:
        '''
        This function gets all the symbols avalable in the binance exchange

        Parameters:
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
    

    def get_historica_data(self, symbol: str, interval: str = '1m', startTime: Optional[int] = None, endTime : Optional[int] = None):

        params = dict()

        params['symbol'] = symbol
        params['interval'] = interval
        params['limit'] = 1000

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
        
            return candles   

        else:
            return None


if __name__ == "__main__":
    bin = BinacneClient(True)
    print(bin.get_historica_data('BTCUSDT'))

