import datetime as dt
import os
import time
import random
import numpy as np
from logging import getLogger

import pandas as pd

logger = getLogger()


BUY_SELL = {'卖出': -1, '买入': 1, 'S': -1, 'B': 1}
def buy_sell2num(x):
    return BUY_SELL[x]


MULTIPLIER = {'IF': 300, 'IH': 300, 'IC': 200, 'NQ': 20}
def multiplier(x):
    first_two = x[:2]
    if first_two in MULTIPLIER.keys():
        return MULTIPLIER[first_two]
    else:
        return 1.0


MARKET_CODE = {'1': '.SZ', '0': '.SH', 'F': '.CFE',
               '0H': '.HK', '1H': '.HK', 'S': '.SHF',
               'D': '.DCE', 'Z': '.CZC', 'N': '.N'}
def market_code(x):
    if not isinstance(x, str):
        x = str(x)
    #x = x.strip()
    return MARKET_CODE[x]


def add_wind_code(ims_df, code_col_name='合约代码', market_col_name='市场'):
    return ims_df.assign(wind_code=lambda x: x[code_col_name] + x[market_col_name].apply(market_code))


MORNING_OPEN = dt.time(hour=9, minute=30)
MORNING_CLOSE = dt.time(hour=11, minute=30)
AFTERNOON_OPEN = dt.time(hour=13)
AFTERNOON_CLOSE = dt.time(hour=15, minute=0)


def trade_time_flag(time_now):
    if isinstance(time_now, dt.datetime):
        time_now = time_now.time()
    elif not isinstance(time_now, dt.time):
        raise Exception('time now must be datetime.datetime or datetime.time!')

    if (((time_now > MORNING_OPEN) and (time_now < MORNING_CLOSE)) or ((time_now > AFTERNOON_OPEN) and (time_now < AFTERNOON_CLOSE))):
        return True
    else:
        return False


def dt2trade_time_float(time, today=dt.datetime.today()):
    assert isinstance(time, dt.datetime)
    assert isinstance(today, dt.datetime)

    output_num = (time.date() - today.date()).days

    if (MORNING_OPEN <= time.time()) and (time.time() <= MORNING_CLOSE):
        seconds = (dt.datetime.combine(today, time.time()) - dt.datetime.combine(today, MORNING_OPEN)).seconds
        output_num += seconds/14400
    elif (AFTERNOON_OPEN < time.time()) and (time.time() <= AFTERNOON_CLOSE):
        seconds = (dt.datetime.combine(today, time.time()) - dt.datetime.combine(today, AFTERNOON_OPEN)).seconds
        output_num += seconds/14400
        output_num += 0.5
    else:
        # 如果不在交易时间则返回 None
        return None

    return output_num


def new_path(path, name=None):
    if os.path.isdir(path):
        return path
    else:
        os.makedirs(path)
        if name:
            print('{} is not exist. Make a new dirctory: {}'.format(path, name))
        return path


def startswith_list(x, starts_list=[]):
    if isinstance(starts_list, list) and isinstance(x, str):
        output_flag = False
        for prefix in starts_list:
            output_flag |= x.startswith(prefix)
    else:
        message = 'starts_list must be a list and x must be a string !'
        raise ValueError(message)
    return output_flag


def is_etf(wind_code):
    if isinstance(wind_code, str):
        market = wind_code.split('.')[-1]
        if market == 'SH':
            if wind_code.startswith('51'):
                return True
            else:
                return False
        elif market == 'SZ':
            if wind_code.startswith('15'):
                return True
            else:
                return False
    else:
        raise ValueError('wind_code must be str !')


def is_lof(wind_code):
    if isinstance(wind_code, str):
        market = wind_code.split('.')[-1]
        if market == 'SH':
            if wind_code.startswith('50'):
                return True
            else:
                return False
        elif market == 'SZ':
            if wind_code.startswith('16'):
                return True
            else:
                return False
    else:
        raise ValueError('wind_code must be str !')


def first2(future_code):
    assert isinstance(future_code, str), 'future_code must be str !'

    return future_code[:2]


def time_label_16():
    '''
    将时间戳乘以10^6再转换成16进制作为时间label，这样不会有重复并且占的字节少
    下单的备注只能加30个字符
    :return:
    '''
    # output = hex(int(time.time()*1e9 + random.random()*1e3))[8:]
    output = hex(int(time.time() * 1e6))[7:] + hex(int(random.random()*1e4))[2:].zfill(4)
    return output


MARKET_WIND2IMS = {'SH': '0', 'SZ': '1'}
def market_wind2ims(wind_code):
    market_code = wind_code.split('.')[-1]
    return MARKET_WIND2IMS[market_code]


def date_yyyymmdd(date=dt.date.today(), date_format='%Y%m%d'):
    if isinstance(date, np.datetime64):
        date = pd.to_datetime(date)

    if isinstance(date, dt.datetime) or isinstance(date, dt.date):
        return date.strftime(date_format)
    elif isinstance(date, int) or isinstance(date, float) or isinstance(date, np.integer):
        date = '{:08.0f}'.format(date)
        try:
            dt.datetime.strptime(date, date_format)
        except Exception as e:
            logger.exception('Can not convert {} to YYYMMDD'.format(date))
            raise e
        return date

    elif isinstance(date, str):
        try:
            dt.datetime.strptime(date, date_format)
        except Exception as e:
            logger.exception('Can not convert {} to YYYMMDD'.format(date))
            raise e
        return date
    else:
        raise ValueError('Can not convert {} to YYYMMDD'.format(date))


def str2date(date):
    if isinstance(date, dt.date):
        return date
    elif isinstance(date, dt.datetime):
        return date.date()
    elif isinstance(date, str):
        date = date.replace('-', '').strip()
        if len(date) > 8:
            date = date[:8]
        try:
            output_date = dt.datetime.strptime(date, '%Y%m%d')
        except Exception as e:
            logger.exception('Can not convert {} to datetime!!!'.format(date))
            raise ValueError('Can not convert {} to datetime!!!'.format(date))
        return output_date.date()
    else:
        raise TypeError('Can not handle this type of date!!!')


def list_split(input_list, max_num=900):
    assert isinstance(input_list, list)
    output_len = int(len(input_list) / max_num)
    output_list = [input_list[i*max_num:(i+1)*max_num] for i in range(output_len)]
    output_list.append(input_list[output_len*max_num:])
    return output_list


def is_index_future(future_code):
    from src.config.hardcode import IndexFutureFirst2
    assert isinstance(future_code, str)
    if future_code[:2] in [e.value for e in IndexFutureFirst2]:
        return True
    else:
        return False


def is_certain_index_future(future_code, future_first2):
    from src.config.hardcode import IndexFutureFirst2
    if isinstance(future_first2, str):
        future_first2 = IndexFutureFirst2[future_first2]
    assert isinstance(future_first2, IndexFutureFirst2)
    assert isinstance(future_code, str)
    if future_code[:2] == future_first2.value:
        return True
    else:
        return False


