# -*- coding: utf-8 -*-

import logging


def get_log_filehandler(fn):
    lh = logging.FileHandler(fn, encoding='utf-8')
    lh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(processName)s - %(message)s')
    lh.setFormatter(formatter)

    return lh


def get_log_console_handler(level=logging.INFO):
    lh = logging.StreamHandler()

    lh.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(processName)s - %(message)s')
    lh.setFormatter(formatter)

    return lh


def get_logger(loggfilename, level=logging.INFO):

    my_logger = logging.getLogger()
    # level = logging.INFO
    my_logger.setLevel(level)
    # my_logger.addHandler(
    #     get_log_filehandler(os.path.join(LOG_DIR, '{}.log'.format(dt.datetime.today().strftime('%Y-%m-%d')))))
    my_logger.addHandler(get_log_filehandler(loggfilename))
    my_logger.addHandler(get_log_console_handler(level=level))

    return my_logger


"""
实施证明这么做并不好
def get_multi_logger(loggfilename):

    my_logger = multiprocessing.get_logger()
    my_logger.setLevel(logging.INFO)
    # my_logger.addHandler(
    #     get_log_filehandler(os.path.join(LOG_DIR, '{}.log'.format(dt.datetime.today().strftime('%Y-%m-%d')))))
    my_logger.addHandler(get_log_filehandler(loggfilename))
    my_logger.addHandler(get_log_console_handler())

    return my_logger
"""




