import pandas as pd
from tqdm import tqdm

from src.data_src.wind_rdf.calender_function import trade_day_list
from src.data_src.wind_rdf.common_func_4_wind_code_date_tables import get_data_one_day_all_ticker


def load_factor_values(begin_date, end_date):
    factor_list = list()
    from src.factor_lib.daily_price.daily_price_factors import CLOSE_FACTOR, CLOSE_NO_ADJ_FACTOR, VOLUME_FACTOR, FACTOR_FACTOR, DAILY_RETURN
    factor_list.append(CLOSE_FACTOR)
    factor_list.append(CLOSE_NO_ADJ_FACTOR)
    factor_list.append(VOLUME_FACTOR)
    factor_list.append(FACTOR_FACTOR)
    factor_list.append(DAILY_RETURN)

    from src.factor_lib.gf_industry_rotation_factor.industry_momentum_factor import GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_DAY_FACTOR, GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_WEEK_FACTOR, GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_MONTH_FACTOR, GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_3_MONTH_FACTOR, GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_6_MONTH_FACTOR
    factor_list.append(GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_DAY_FACTOR)
    factor_list.append(GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_WEEK_FACTOR)
    factor_list.append(GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_1_MONTH_FACTOR)
    factor_list.append(GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_3_MONTH_FACTOR)
    factor_list.append(GF_INDUSTRY_MOMENTUM_CITIC_LEVEL_2_6_MONTH_FACTOR)

    output_list = list()

    date_list = trade_day_list(start_date=begin_date, end_date=end_date)
    date_bar = tqdm(date_list)
    for date in date_bar:
        wind_code_list = get_stock_list(date)
        for wind_code in wind_code_list:
            date_bar.set_description(f'Calculating factors of {wind_code} on {date}:')

            factor_value_dict = {factor.get_factor_name(): factor.cal_factor_once(wind_code=wind_code, date=date) for factor in factor_list}
            factor_value_dict['wind_code'] = wind_code
            factor_value_dict['date'] = date
            output_list.append(factor_value_dict)
    df = pd.DataFrame(output_list)
    return df


def get_stock_list(date):

    df = get_data_one_day_all_ticker(table_name='AShareEODPrices',
                                     date=date,
                                     fields='S_INFO_WINDCODE,TRADE_DT,S_DQ_TRADESTATUSCODE',
                                     )
    df = df[df['S_DQ_TRADESTATUSCODE'] != 0]
    return df['S_INFO_WINDCODE'].to_list()