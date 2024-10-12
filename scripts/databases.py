import os
import sys
from typing import *
import logging
import time

import psycopg2
import h5py 
import numpy as np
import pandas as pd


logger = logging.getLogger()

class Database:
    
    def __init__(self, exchange: str, hdf5: bool = True):

        self.hdf5 = hdf5

        if self.hdf5:

            filePath = os.path.join(os.getcwd(), 'data')
            if not os.path.exists(filePath):
                os.mkdir(filePath)

            self.hf = h5py.File(f"data/{exchange}.h5", 'a')
            self.hf.flush()

    
    def create_dataset(self, symbol: str):
        '''
        This is used to create a dataset inside the .h5 file:

        Parameter:
        ---------
            symbol(str): symbol/ticker 
        '''
        if symbol not in self.hf.keys():
            self.hf.create_dataset(symbol,shape=(0,6),maxshape=(None, 6), dtype='float64')
            self.hf.flush()


    def write_data(self, symbol: str, data: List[tuple]):
        '''
        This function is used to write data to the HDF5 by resizing the existing dataset.
        The funtion first checks if the data that is sent is has a min timestamp compared to the existing data and
        if also checks if the data that is sent has a max ts greater than the existing  data

        Parameter:
        ---------
            symbol(str): 
            data(List[tuple]): a tuple consisting of Open time, open, high, low, close prices and volume data
        '''

        min_ts, max_ts = self.get_first_last_timestamp(symbol)

        if min_ts is None:
            min_ts = float('inf')
            max_ts = float(0)

        filtered_data = []

        for d in data:
            if d[0] < min_ts:
                filtered_data.append(d)
            elif d[0] > max_ts:
                filtered_data.append(d)

        if len(filtered_data) == 0:
            logger.warning('%s: No data to insert',symbol)
            return

        data_array = np.array(filtered_data)

        self.hf[symbol].resize(self.hf[symbol].shape[0] + data_array.shape[0], axis=0)
        self.hf[symbol][-data_array.shape[0]:] = data_array

        self.hf.flush()

    def get_data(self, symbol: str, from_time: int, to_time: int)->Union[None, pd.DataFrame]:
        '''
        This function is used to retreive data from database

        Parameter:
        ---------
            symbol(str): 
            from_time(int): time of start in seconds
            to_time(int): time of end in seconds

        Return:
        -------
            None: if no data exists in that time period
            pd.DataFrame: A data frame consisting of timestamp as index and open, high, low, close, volume of a particular symbol
        '''

        start_query = time.time()

        existing_data = self.hf[symbol][:]

        if len(existing_data) == 0:
            return None
        
        data = sorted(existing_data, key=lambda x: x[0])
        data = np.array(data)

        df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df = df[(df['Timestamp'] >= from_time * 1000) & (df['Timestamp'] <= to_time * 1000)]

        df['Timestamp'] = pd.to_datetime(df['Timestamp'].astype('int64'), unit='ms')
        df.set_index('Timestamp', drop=True, inplace=True)


        query_time = round((time.time() - start_query), 2)

        logger.info("Retrieved %s %s data from database in %s", len(df), symbol, query_time)
        
        return df

    def get_first_last_timestamp(self, symbol:str)->Union[Tuple[None, None], Tuple[float, float]]:
        '''
        This function is used to return the most recent and oldest timestamp

        Parameter:
        ----------
            symbos(str)

        Return:
        -------
            oldest timestap(float): the first timestapm
            recenct timestamp(float): the last timestamp
        '''

        existing_data = self.hf[symbol][:]

        if len(existing_data) == 0:
            return None, None
        
        first_ts = min(existing_data, key=lambda x: x[0])[0]
        last_ts = max(existing_data, key=lambda x: x[0])[0]

        return first_ts, last_ts
