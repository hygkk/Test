#!/home/wzf/.conda/envs/env_wzf/bin/python
import unittest
import os
import datetime as dt
import logging

from src.utils.log_utils import get_logger
from src.utils.sys_utils import multiprocessing_start_modes


test_log_dir = os.path.join(os.path.pardir, 'test_log')
if not os.path.isdir(test_log_dir):
    os.makedirs(test_log_dir)

strftime = dt.datetime.today().strftime("%Y%m%d%H%M%S")
logger = get_logger(os.path.join(test_log_dir, 'test_{}.log'.format(strftime)), level=logging.INFO)

test_output = os.path.join(test_log_dir, 'test_output_{}.log'.format(strftime))

if __name__ == '__main__':
    test_dir = './test'
    discover = unittest.defaultTestLoader.discover(test_dir, pattern="test*.py")

    # test_log_dir = os.path.join(os.path.pardir, 'test_log')
    # if not os.path.isdir(test_log_dir):
    #     os.makedirs(test_log_dir)
    # with open(os.path.join(os.path.pardir, 'test_log', 'test_{}.log'.format(dt.datetime.today().strftime("%Y%m%d%H%M%S"))), 'w') as f:
    #     runner = unittest.TextTestRunner(stream=f, verbosity=2)
    #     runner.run(discover)
    # runner = unittest.TextTestRunner(verbosity=2)
    # runner.run(discover)
    with open(test_output, 'a') as f:
        runner = unittest.TextTestRunner(stream=f, verbosity=2)
        runner.run(discover)