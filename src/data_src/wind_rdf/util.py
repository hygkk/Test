import datetime as dt
import time

import pandas as pd
import logging
from copy import copy
import numpy as np
from decimal import Decimal
from functools import lru_cache
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
# from functools import lru_cache

# from . import CURSOR, con
# from . import con_pool
from . import ENGINE
from src.utils.base_class import StringEnum

logger = logging.getLogger()


def convert_2_date_str(date):
    if isinstance(date, dt.datetime) or isinstance(date, dt.date):
        date_str = date.strftime('%Y%m%d')
    elif isinstance(date, str):
        assert len(date) == 8, 'Error: date_str format is incorrect !!! date_str = {}'.format(date)
        date_str = date
    elif isinstance(date, np.datetime64):
        # tmp = date.astype(dt.datetime)
        # date_str = tmp.strftime('%Y%m%d')
        date_str = pd.to_datetime(str(date)).strftime('%Y%m%d')
    elif isinstance(date, np.int64):
        # tmp = date.astype(dt.datetime)
        # date_str = tmp.strftime('%Y%m%d')
        date_str = pd.to_datetime(str(date)).strftime('%Y%m%d')
    else:
        raise ValueError('This type can not convert to date_str !!!\n date = {}\n type(date) = {}'.format(date, type(date)))
    return date_str


def str_join(input_list):
    '''
    把 list 转换成带引号的字符串
    :param input_list: [ 'a', 'b', 'c']
    :return:  "'a','b','c'"
    '''
    output_str = "','".join(input_list)
    output_str = "'" + output_str + "'"
    return output_str


def str_join_under_1000(input_list):
    """
    数据库显示查找的时候where in 之后的列表长度不能超过1000
    这个函数返回一个字符串列表，每个里面有1000个
    :param input_list:
    :return:
    """
    assert isinstance(input_list, list), 'input_list must be list !!! \ninput_list = {}'.format(input_list)
    the_list = copy(input_list)
    output_str_list = []
    while len(the_list) > 999:
        output_str_list.append(str_join(the_list[:999]))
        the_list = copy(the_list[999:])
    output_str_list.append(str_join(the_list))
    return output_str_list

# 不要自己实现
# def sql_2_pddf(sql):
#     assert isinstance(sql, str)
#     logger.info('sql = {}'.format(sql))
#     CURSOR.execute(sql)
#     raw_data = CURSOR.fetchall()
#     col_names = [i[0] for i in CURSOR.description]
#     output_df = pd.DataFrame(data=raw_data, columns=col_names)
#     return output_df


def sql_2_pddf(sql, retry_n=20, sleep_seconds=10):
    # con = con_pool.acquire()
    assert isinstance(sql, str)
    logger.info('sql = {}'.format(sql))
    try:
        with ENGINE.connect() as conn:
            output_df = pd.read_sql(sql=sql, con=conn)
        return output_df
    except OperationalError as err:
        logger.exception(f'Operational Error! Try {retry_n} times!')
        for i in range(retry_n):
            try:
                time.sleep(sleep_seconds)
                with ENGINE.connect() as conn:
                    output_df = pd.read_sql(sql=sql, con=conn)
                return output_df
            except OperationalError as e:
                logger.exception(f'{i}th retry failed!')
        raise err


def sql_raw(sql):
    with ENGINE.connect() as conn:
        # con = con_pool.acquire()
        # cursor = con.cursor()
        assert isinstance(sql, str)
        logger.info('sql = {}'.format(sql))
        result_proxy = conn.execute(text(sql))
        raw_data = result_proxy.fetchall()
        raw_data = convert_decimal2flot_raw_data(raw_data)
        # cursor.close()
        # con_pool.release(con)
    return raw_data


def convert_decimal2flot_raw_data(raw_data):
    output_data = []
    for row in raw_data:
        new_row = [float(x) if isinstance(x, Decimal) else x for x in row]
        output_data.append(tuple(new_row))
    return tuple(output_data)


def sql_2_dict(sql):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    assert isinstance(sql, str)

    df = sql_2_pddf(sql)
    if df.shape[0] == 1:
        return df.iloc[0].to_dict()
    elif df.shape[1] == 1:
        return df.T.iloc[0].to_dict()
    return df.to_dict()


def get_yesterday(date):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    date_str = convert_2_date_str(date)
    sql = "select max(TRADE_DAYS) from wind_filesync.AShareCalendar where S_INFO_EXCHMARKET = 'SSE' and TRADE_DAYS < {}".format(date_str)
    # cursor.execute(sql)
    # raw_data = cursor.fetchall()
    raw_data = sql_raw(sql)
    assert len(raw_data) == 1, 'Error: raw_data = {}'.format(raw_data)
    assert len(raw_data[0]) == 1, 'Error: raw_data = {}'.format(raw_data)
    output = raw_data[0][0]
    assert isinstance(output, str), 'Error: get_yesterday = {}'.format(output)
    output = dt.datetime.strptime(output, '%Y%m%d')
    # cursor.close()
    # con_pool.release(con)
    return output


def get_tomorrow(date):
    # con = con_pool.acquire()
    with ENGINE.connect() as conn:
        # cursor = con.cursor()
        date_str = convert_2_date_str(date)
        sql = "select min(TRADE_DAYS) from wind_filesync.AShareCalendar where S_INFO_EXCHMARKET = 'SSE' and TRADE_DAYS > {}".format(date_str)
        result_proxy = conn.execute(sql)
        raw_data = result_proxy.fetchall()
    assert len(raw_data) == 1, 'Error: raw_data = {}'.format(raw_data)
    assert len(raw_data[0]) == 1, 'Error: raw_data = {}'.format(raw_data)
    output = raw_data[0][0]
    assert isinstance(output, str), 'Error: get_yesterday = {}'.format(output)
    output = dt.datetime.strptime(output, '%Y%m%d')
        # cursor.close()
        # con_pool.release(con)
    return output


def is_trade_day(date):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    date_str = convert_2_date_str(date)
    sql = "select * from wind_filesync.AShareCalendar where S_INFO_EXCHMARKET = 'SSE' and TRADE_DAYS = {}".format(date_str)
    # cursor.execute(sql)
    # raw_data = cursor.fetchall()
    raw_data = sql_raw(sql)
    # cursor.close()
    # con_pool.release(con)
    if len(raw_data) == 0:
        return False
    elif len(raw_data) == 1:
        return True
    else:
        raise Exception('Error: raw_data = {}'.format(raw_data))


@lru_cache(maxsize=30)
def trade_day_list(begin_date, end_date):
    begin_date_str = convert_2_date_str(begin_date)
    end_date_str = convert_2_date_str(end_date)
    sql = f"SELECT " \
          f"    TRADE_DAYS " \
          f"FROM" \
          f"    wind_filesync.AShareCalendar " \
          f"WHERE " \
          f"    S_INFO_EXCHMARKET = 'SSE' " \
          f"        AND TRADE_DAYS >= '{begin_date_str}' " \
          f"        AND TRADE_DAYS <= '{end_date_str}' " \
          f"ORDER BY TRADE_DAYS"
    raw_data = sql_raw(sql)
    day_list = tuple(x[0] for x in raw_data)
    return day_list


def date2str(date):
    if isinstance(date, dt.datetime) or isinstance(date, dt.date):
        date_str = date.strftime('%Y%m%d')
    elif isinstance(date, str):
        assert len(date) == 8, 'date must be like yyyymmdd !!!'
        date_str = date
    else:
        raise TypeError('Can not handle this type of date !!!')

    return date_str


def month2str(month):
    if isinstance(month, str):
        month_date = dt.datetime.strptime(month.strip().replace('-', '').replace('/', ''), "%Y%m")
        return month_date.strftime("%Y%m")
    if isinstance(month, dt.datetime) or isinstance(month, dt.date):
        return month.strftime("%Y%m")
    else:
        raise TypeError('Can not convert this type to month_str')


class CacheMode(StringEnum):
    no_cache = 'no_cache'
    cache_one_day = 'cache_one_day'
    cache_one_ticker_all_days = 'cache_one_ticker_all_days'
    cache_one_day_all_fields = 'cache_one_day_all_fields'
    cache_one_month_all_fields = 'cache_one_month_all_fields'
    cache_one_year_all_fields = 'cache_one_year_all_fields'
    cache_all_ticker_all_days = 'cache_all_ticker_all_days'
    cache_entire_table = 'cache_entire_table'
    cache_tickers_all_days = 'cache_tickers_all_days'


