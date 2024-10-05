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


    def get_symbols(self):

        params = dict()

        endpoint = '/fapi/v1/exchangeInfo' if self.futures else '/api/v1/exchangeInfo'

        data = self._make_request(endpoint, params)

        symbols_data = data['symbols']
        symbols = []
        
        # Only collect symbols that have a quote asset of USDT because they have a higher volume
        for symbol in symbols_data:
            if symbol['quoteAsset'] == 'USDT':
                symbols.append(symbol['symbol'])

        return symbols




if __name__ == "__main__":
    bin = BinacneClient(False)
    print(bin.get_symbols())

