import os
import sys
import logging


logger = logging.getLogger()


log_dir = os.path.join(os.getcwd(), 'logs')
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

formatter = logging.Formatter('%(asctime)s - %(levelname)s :: %(message)s')


log_file_error = os.path.join(log_dir, 'Error.log')
log_file_info = os.path.join(log_dir, 'Info.log')


info_handler = logging.FileHandler(log_file_info)
error_handler = logging.FileHandler(log_file_error)

info_handler.setLevel(logging.DEBUG)
error_handler.setLevel(logging.ERROR)


info_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)


logger.addHandler(info_handler)
logger.addHandler(error_handler)
