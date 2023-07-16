"""
此处因子来自于 广发证券研报
《考虑拥挤度的细分行业动量策略 ——重构量化行业轮动框架：技术篇》
中的动量相关策略

原研报中采用的是申万二级行业，这里采用中信二级行业，主要是因为申万二级行业竟然不包括科创板
"""
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


logger = getLogger()


@lru_cache(maxsize=50)
def cal_industry_return_ranking(date, days_interval: int):
    index_info_df = get_industry_index_info(create_index_organization_code=1052, public_index_type=161002, end_date_null_flag=False)
    date_str = date_yyyymmdd(date)
    begin_date = t_days_off(date, - days_interval)
    return_dict = dict()
    # i_tqdm = tqdm(index_info_df.index.to_list())
    index_wind_code_list = list()
    for i in index_info_df.index:
        wind_code = index_info_df.loc[i, 'SecuCode'] + '.' + MARKET_CODE_NUM_DICT.inverse[
            index_info_df.loc[i, 'SecuMarket']]
        index_wind_code_list.append(wind_code)
    index_wind_code_tuple = tuple(index_wind_code_list)

    for i in index_info_df.index:

        wind_code = index_info_df.loc[i, 'SecuCode'] + '.' + MARKET_CODE_NUM_DICT.inverse[index_info_df.loc[i, 'SecuMarket']]
        # i_tqdm.set_description(f'Calculating return of {wind_code}')
        try:
            now_close = gildata_one_day_one_ticker(wind_code=wind_code,
                                                   date=date_str,
                                                   table_name='QT_IndexQuote',
                                                   field='ClosePrice',
                                                   cache_mode=CacheMode.cache_tickers_all_days,
                                                   wind_codes_used_4_cache=index_wind_code_tuple,
                                                   log_exception_flag=False,
                                                   )

            begin_close = gildata_one_day_one_ticker(wind_code=wind_code,
                                                     date=begin_date.strftime("%Y%m%d"),
                                                     table_name='QT_IndexQuote',
                                                     field='ClosePrice',
                                                     cache_mode=CacheMode.cache_tickers_all_days,
                                                     wind_codes_used_4_cache=index_wind_code_tuple,
                                                     log_exception_flag=False,
                                                     )

            return_dict[wind_code] = now_close / begin_close - 1.0
        except KeyError as e:
            logger.debug(f'Maybe {wind_code} was not listed on {begin_date}')

    output_df = pd.DataFrame({'return': return_dict})
    output_df['return_ranking'] = output_df['return'].rank() / output_df.shape[0]
    return output_df


class GFIndustryMomentumCITICLevel2(Factor):
    factor_type = GF_INDUSTRY_ROTATION_FACTORS

    def __init__(self, days_interval: int):
        super(GFIndustryMomentumCITICLevel2, self).__init__()

        self.name = f'GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_{days_interval}_DAYS'
        self.description = f'此处因子来自于 广发证券研报《考虑拥挤度的细分行业动量策略 ——重构量化行业轮动框架：技术篇》中的动量相关策略。收益率计算时长为{days_interval}天'
        self.formular = f' industry_index_close_(T0) /  industry_index_close_(T - {days_interval}) - 1.0'

        self.days_interval = days_interval

    # @time_this
    def cal_factor_once(self, wind_code, date, cache_mode=CacheMode.cache_one_month_all_fields):
        industry_index_code = get_stock_industry_class(wind_code=wind_code, date=date)
        if industry_index_code is not None:
            index_return_ranking_df = cal_industry_return_ranking(date=date, days_interval=self.days_interval)
            if industry_index_code in index_return_ranking_df.index:
                return index_return_ranking_df.loc[industry_index_code, 'return_ranking']
            else:
                # 该行业指数开始有数据的时间还不足days_interval，平均值代替
                return 0.5
        else:
            # 还没有行业分类的返回0.5，也就是平均值
            return 0.5


GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_DAY_FACTOR = GFIndustryMomentumCITICLevel2(days_interval=1)
GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_WEEK_FACTOR = GFIndustryMomentumCITICLevel2(days_interval=N_TRADE_DAYS_PER_WEEK)
GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_MONTH_FACTOR = GFIndustryMomentumCITICLevel2(days_interval=N_TRADE_DAYS_PER_MONTH)
GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_3_MONTH_FACTOR = GFIndustryMomentumCITICLevel2(days_interval=N_TRADE_DAYS_PER_MONTH * 3)
GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_6_MONTH_FACTOR = GFIndustryMomentumCITICLevel2(days_interval=N_TRADE_DAYS_PER_MONTH * 6)

