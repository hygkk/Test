import pandas as pd
from tqdm import tqdm
from functools import lru_cache
from logging import getLogger

from src.config import N_TRADE_DAYS_PER_WEEK, N_TRADE_DAYS_PER_MONTH
from src.data_src.wind_rdf.calender_function import t_days_off
from src.data_src.wind_rdf.gildata_industry_index import get_industry_index_info, MARKET_CODE_NUM_DICT, \
    get_stock_industry_class
from src.data_src.wind_rdf.common_func_4_gildata import gildata_one_day_one_ticker
from src.data_src.wind_rdf.util import CacheMode
from src.factor_lib.factor_base_class import Factor
from gf_industry_rotation_factor import GF_INDUSTRY_ROTATION_FACTORS
from src.utils.utils import date_yyyymmdd
import openpyxl
import statsmodels.api as sm


class ZYIndustryMomentum(Factor):
    def __init__(self, days_interval: int):
        super().__init__()
        self.name = f'ZY_INDUSTRY_MOMENTUM{days_interval}_DAYS'
        self.description = f'此处因子来自于中银证券研报《如何把握市场“未证伪情绪”构建行业动量策略》中的相关策略。形成期长度为{days_interval}天'
        self.formular = f'行业日收益率对行业日换手率变化率回归得到残差'
        self.interval = days_interval

    def cal_factor_once(self,wind_code,date,cache_mode=CacheMode.cache_one_day_all_fields):
        industry=get_stock_industry_class(wind_code=wind_code)
        rtn = []
        roc_of_turnover_rate = []
        begin_date = date_yyyymmdd(t_days_off(date=date, n=-self.interval))
        try:
            for i in range(self.interval):
                next_date=date_yyyymmdd(t_days_off(date=begin_date,n=1))
                begin_close=gildata_one_day_one_ticker(wind_code=industry,
                                                       date=begin_date,
                                                       table_name='QT_IndexQuote',
                                                       field='ClosePrice')
                now_close=gildata_one_day_one_ticker(wind_code=wind_code,
                                                     date=next_date,
                                                     table_name='QT_IndexQuote',
                                                     field='ClosePrice')
                rtn.append(now_close/begin_close-1)
                begin_turnover=gildata_one_day_one_ticker(wind_code=wind_code,
                                                          date=begin_date,
                                                          table_name='QT_IndexQuote',
                                                          field='TurnoverVolume')
                now_turnover = gildata_one_day_one_ticker(wind_code=wind_code,
                                                          date=next_date,
                                                          table_name='QT_IndexQuote',
                                                          field='TurnoverVolume')
                roc_of_turnover_rate.append(now_turnover/begin_turnover-1)
                begin_date=next_date
            model=sm.OLS(rtn,roc_of_turnover_rate).fit()
            factor=1
            for j in model.resid:
                factor*=1+j
            return factor
        except:
            return None


test=ZYIndustryMomentum(30)
print(test.cal_factor_once('601857.SH','20160705')) # 中石油