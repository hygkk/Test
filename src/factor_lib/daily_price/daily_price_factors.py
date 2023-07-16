from logging import getLogger

from ..factor_base_class import Factor
from src.data_src.wind_rdf.market_price import get_eod_price_one_day_one_ticker, get_newest_eod_date_one_ticker_one_field
# from src.static_data.calender import my_calender
from src.data_src.wind_rdf.util import CacheMode
from src.utils.wrappers import time_this


logger = getLogger()


class DailyEODBasicFactor(Factor):

    def __init__(self, name, description, formular,
                 rdf_used_field=None,
                 rdf_unit=1,                    # wind rdf里面有的单位不是1， 是千元之类的
                 ):
        super(DailyEODBasicFactor, self).__init__()
        assert isinstance(name, str)
        assert isinstance(description, str)
        assert isinstance(formular, str)
        self.name = name
        self.description = description
        self.formular = formular
        self.rdf_used_field = rdf_used_field
        self.rdf_unit = rdf_unit
        self.factor_type = 'Daily_Base_Price'

    # @time_this
    def cal_factor_once(self, wind_code, date, cache_mode='cache_one_day'):
        try:
            return get_eod_price_one_day_one_ticker(wind_code=wind_code,
                                                    date=date,
                                                    cache_mode=cache_mode,
                                                    field=self.rdf_used_field) * self.rdf_unit
        except KeyError as e:
            logger.warning(f'Can not calculate {self.name} of {wind_code} on {date}')

            if self.name in ('close', 'open', 'high', 'low', 'factor', 'vwap', 'close_no_adj'):
                return self.cal_newest_factor(wind_code=wind_code, date=date)
                # pass
            elif self.name in ('volume', 'amount', 'trade_status'):
                return 0
            else:
                raise ValueError(f'Unknown name: {self.name}')

    def cal_newest_factor(self, wind_code, date):
        return get_newest_eod_date_one_ticker_one_field(wind_code=wind_code,
                                                        date=date,
                                                        field=self.rdf_used_field) * self.rdf_unit


CLOSE_FACTOR = DailyEODBasicFactor(name='close',
                                   description='收盘价，实际上是复权收盘价，方便回测',
                                   formular='无需计算，直接从数据库中提取',
                                   rdf_used_field='S_DQ_ADJCLOSE')

CLOSE_NO_ADJ_FACTOR = DailyEODBasicFactor(name='close_no_adj',
                                          description='收盘价，不复权',
                                          formular='无需计算，直接从数据库中提取',
                                          rdf_used_field='S_DQ_CLOSE')

VOLUME_FACTOR = DailyEODBasicFactor(name='volume',
                                    description='成交量',
                                    formular='无需计算，直接从数据库中提取',
                                    rdf_used_field='S_DQ_VOLUME',
                                    rdf_unit=100)

FACTOR_FACTOR = DailyEODBasicFactor(name='factor',
                                    description='复权因子',
                                    formular='无需计算，直接从数据库中提取',
                                    rdf_used_field='S_DQ_ADJFACTOR')


class DailyReturn(Factor):
    name = 'daily_return'
    description = '股票的每日收益'
    formular = 'close / preclose - 1\n当日收盘价相比于昨收的涨跌幅，注意，昨收是复权过的'
    factor_type = 'Daily_Base_Price'

    def cal_factor_once(self, wind_code, date, cache_mode=CacheMode.cache_one_day_all_fields):
        field_name_close = 'S_DQ_CLOSE'
        field_name_preclose = 'S_DQ_PRECLOSE'
        try:

            close = get_eod_price_one_day_one_ticker(wind_code=wind_code,
                                                     date=date,
                                                     cache_mode=cache_mode,
                                                     field=field_name_close)

            preclose = get_eod_price_one_day_one_ticker(wind_code=wind_code,
                                                        date=date,
                                                        cache_mode=cache_mode,
                                                        field=field_name_preclose)
            daily_return = (close / preclose - 1)
            return daily_return
        except Exception as e:
            return 0.0


DAILY_RETURN = DailyReturn()






