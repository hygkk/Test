'''
获取交易所交易日历
'''
import datetime as dt
from functools import lru_cache

from .util import sql_raw


def _date_8_str(date):
    if isinstance(date, dt.date) or isinstance(date, dt.datetime):
        return date.strftime('%Y%m%d')
    elif isinstance(date, str):
        date_str = date.replace('-', '')
        assert len(date_str) == 8, 'Date format must be YYYYMMDD !!!'
        return date_str
    else:
        raise ValueError('Con not convert this type of date !!! type = {}'.format(type(date)))


def _8_str_date(date_str):
    return dt.datetime.strptime(date_str, '%Y%m%d').date()


@lru_cache(maxsize=2)
def get_calender():
    sql = "select TRADE_DAYS from wind_filesync.AShareCalendar where S_INFO_EXCHMARKET = 'SSE' order by TRADE_DAYS"
    raw_data = sql_raw(sql=sql)
    calender = []
    for date in raw_data:
        calender.append(date[0])
    return calender


def is_trade_day(date=dt.date.today()):
    raw_data = get_calender()
    date = _date_8_str(date=date)
    return date in raw_data


def get_tomorrow(date=dt.date.today()):
    # assert is_trade_day(date=date), 'Today is not trade day !!! {}'.format(date)
    # raw_data = get_calender()
    # date = _date_8_str(date=date)
    # idx = raw_data.index(date)
    # return _8_str_date(raw_data[idx + 1])
    return t_days_off(date=date, n=1)


def t_days_off(date=dt.date.today(), n=1):
    assert is_trade_day(date=date), 'Today is not trade day !!! {}'.format(date)
    assert isinstance(n, int)
    raw_data = get_calender()
    date = _date_8_str(date=date)
    idx = raw_data.index(date)
    return _8_str_date(raw_data[idx + n])


def trade_day_list(start_date, end_date, output_type='datetime.date'):
    start_date = _date_8_str(start_date)
    end_date = _date_8_str(end_date)
    raw_data = get_calender()
    if output_type == 'datetime.date':
        output_list = [_8_str_date(v) for v in raw_data if ((v >= start_date) and (v <= end_date))]
        return output_list
    elif output_type == 'str':
        return [v for v in raw_data if ((v >= start_date) and (v <= end_date))]
    else:
        raise NotImplementedError('Do not know this output type: {}'.format(output_type))
