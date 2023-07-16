import datetime as dt
import logging
import pandas as pd
from functools import lru_cache

# from . import CURSOR
# from . import con_pool
from . import ENGINE
from .util import convert_2_date_str, str_join, str_join_under_1000, sql_2_pddf, get_tomorrow, sql_raw, CacheMode, sql_2_dict
# from src.utils.utils import str2date


logger = logging.getLogger()


def get_stock_close_price_one_day(wind_code, date):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    logger.debug('Get close of {} on {}'.format(wind_code, date))
    assert isinstance(wind_code, str)
    # if isinstance(date, dt.datetime) or isinstance(date, dt.date):
    #     date = date.date()
    #     date_str = date.strftime('%Y%m%d')
    # elif isinstance(date, str):
    #     date_str = date
    #     assert len(date_str) == 8, 'Error: date_str = {}'.format(date_str)
    date_str = convert_2_date_str(date)
    sql = "select S_DQ_CLOSE from wind_filesync.AShareEODPrices where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(wind_code, date_str)
    # cursor.execute(sql)
    # raw_data = cursor.fetchall()
    # cursor.close()
    # con_pool.release(con)
    raw_data = sql_raw(sql)
    assert len(raw_data) == 1, 'Error in length of raw_date: {}'.format(raw_data)
    assert len(raw_data[0]) == 1, 'Error in length of raw_date: {}'.format(raw_data)

    output = raw_data[0][0]
    assert isinstance(output, float)
    return output


def get_eod_price_one_day_one_ticker(wind_code, date, field='S_DQ_CLOSE', cache_mode='no_cache'):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    logger.debug('Get {} of {} on {}'.format(field, wind_code, date))
    assert isinstance(wind_code, str)
    assert isinstance(field, str)
    cache_mode = CacheMode.turn2this(cache_mode)
    # if isinstance(date, dt.datetime) or isinstance(date, dt.date):
    #     date = date.date()
    #     date_str = date.strftime('%Y%m%d')
    # elif isinstance(date, str):
    #     date_str = date
    #     assert len(date_str) == 8, 'Error: date_str = {}'.format(date_str)
    date_str = convert_2_date_str(date)
    if cache_mode == CacheMode.no_cache:
        sql = "select {} from wind_filesync.AShareEODPrices where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(field, wind_code, date_str)
        # cursor.execute(sql)
        # raw_data = cursor.fetchall()
        # cursor.close()
        # con_pool.release(con)
        raw_data = sql_raw(sql)
        assert len(raw_data) == 1, 'Error in length of raw_date: {}'.format(raw_data)
        assert len(raw_data[0]) == 1, 'Error in length of raw_date: {}'.format(raw_data)

        output = raw_data[0][0]
        assert isinstance(output, float)
        return output
    elif cache_mode == CacheMode.cache_one_day:
        all_stock_df = get_eod_price_one_day(date=date, fields=field, set_index='S_INFO_WINDCODE')

        return all_stock_df.loc[wind_code, field]
    elif cache_mode == CacheMode.cache_one_day_all_fields:
        # year_month = date_str[:6]
        # begin_date_str = year_month + '01'
        # end_date_str = year_month + '31'
        all_stock_df = get_eod_price_period(wind_codes=None,
                                            begin_date=date_str,
                                            end_date=date_str,
                                            fields='*',
                                            set_index=('S_INFO_WINDCODE', 'TRADE_DT'))
        return all_stock_df.loc[(wind_code, date_str), field]
    elif cache_mode == CacheMode.cache_one_month_all_fields:
        year_month = date_str[:6]
        begin_date_str = year_month + '01'
        end_date_str = year_month + '31'
        all_stock_df = get_eod_price_period(wind_codes=None,
                                            begin_date=begin_date_str,
                                            end_date=end_date_str,
                                            fields='*',
                                            set_index=('S_INFO_WINDCODE', 'TRADE_DT'))
        return all_stock_df.loc[(wind_code, date_str), field]
    elif cache_mode == CacheMode.cache_one_year_all_fields:
        year = date_str[:4]
        begin_date_str = year + '0101'
        end_date_str = year + '1231'
        all_stock_df = get_eod_price_period(wind_codes=None,
                                            begin_date=begin_date_str,
                                            end_date=end_date_str,
                                            fields='*',
                                            set_index=('S_INFO_WINDCODE', 'TRADE_DT'))
        return all_stock_df.loc[(wind_code, date_str), field]
    else:
        raise NotImplementedError('Do not know this cache mode.')


@lru_cache(maxsize=100)
def get_newest_eod_data_one_ticker(wind_code, date):
    date_str = convert_2_date_str(date)
    sql = f"SELECT " \
          f"    *" \
          f"FROM " \
          f"    wind_filesync.AShareEODPrices " \
          f"WHERE " \
          f"    S_INFO_WINDCODE = '{wind_code}' " \
          f"        AND TRADE_DT = (SELECT  " \
          f"            MAX(TRADE_DT)" \
          f"        FROM" \
          f"            wind_filesync.AShareEODPrices" \
          f"        WHERE" \
          f"            S_INFO_WINDCODE = '{wind_code}'" \
          f"                AND trade_dt <= '{date_str}')"
    output_dict = sql_2_dict(sql)
    return output_dict


def get_newest_eod_date_one_ticker_one_field(wind_code, date, field):
    raw_dict = get_newest_eod_data_one_ticker(wind_code=wind_code, date=date)
    output_date = raw_dict[field]
    assert not isinstance(output_date, dict), f'Can not get newest {field} of {wind_code} on {date}'
    return output_date


@lru_cache(maxsize=500)
def get_eod_price_one_day(wind_codes=None, date=None, fields='*', set_index=None):
    assert date is not None
    if wind_codes is None:
        logger.info('Get close of all stock on {}'.format(date))
        # assert isinstance(wind_codes, list)
        date_str = convert_2_date_str(date)
        # wind_code_str_list = str_join_under_1000(wind_codes)
        # tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
        sql = "select S_INFO_WINDCODE, {} from wind_filesync.AShareEODPrices where TRADE_DT = '{}'".format(
            fields, date_str)
    else:
        logger.info('Get close of {} on {}'.format(wind_codes, date))
        assert isinstance(wind_codes, list)
        # if isinstance(date, dt.datetime) or isinstance(date, dt.date):
        #     date = date.date()
        #     date_str = date.strftime('%Y%m%d')
        # elif isinstance(date, str):
        #     date_str = date
        #     assert len(date_str) == 8, 'Error: date_str = {}'.format(date_str)
        date_str = convert_2_date_str(date)
        # wind_codes_str = str_join(wind_codes)
        # sql = "select {} from wind_filesync.AShareEODPrices where S_INFO_WINDCODE in ({}) and TRADE_DT = '{}'".format(fields, wind_codes_str, date_str)
        # logger.info('spl = \n{}'.format(sql))
        # # try:
        #     CURSOR.execute(sql)
        # except Exception as e:
        #     # TODO: except后面的代码功能完全可以覆盖前面的代码，测试一段时间之后统一起来
        #     logger.exception('Exception logged !!!')
        wind_code_str_list = str_join_under_1000(wind_codes)
        tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
        sql = "select S_INFO_WINDCODE, {} from wind_filesync.AShareEODPrices where (S_INFO_WINDCODE in ({})) and TRADE_DT = '{}'".format(
            fields, tmp_str, date_str)
    output_df = sql_2_pddf(sql=sql)
    if set_index is not None:
        output_df.set_index(set_index, inplace=True)
    return output_df


@lru_cache(maxsize=100)
def get_eod_price_period(wind_codes=None, begin_date=None, end_date=None, fields='*', set_index=None):
    assert begin_date is not None
    assert end_date is not None
    begin_date_str = convert_2_date_str(begin_date)
    end_date_str = convert_2_date_str(end_date)
    if fields != '*':
        fields += ', S_INFO_WINDCODE, TRADE_DT'
    if wind_codes is None:
        logger.info('Get eod data of all stocks')
        # assert isinstance(wind_codes, list)

        sql = "select {} from wind_filesync.AShareEODPrices where TRADE_DT >= '{}' and TRADE_DT <= '{}'".format(
            fields, begin_date_str, end_date_str)
    else:
        logger.info('Get eod data of {} from {} to {}'.format(wind_codes, begin_date, end_date))
        if isinstance(wind_codes, str):
            wind_codes = [wind_codes]
        assert isinstance(wind_codes, list)

        wind_code_str_list = str_join_under_1000(wind_codes)
        tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
        sql = "select {} from wind_filesync.AShareEODPrices where (S_INFO_WINDCODE in ({})) and TRADE_DT >= '{}' and TRADE_DT <= '{}'".format(
            fields, tmp_str, begin_date, end_date)
    output_df = sql_2_pddf(sql=sql)
    if set_index is not None:
        if isinstance(set_index, tuple):
            set_index = list(set_index)
        output_df.set_index(set_index, inplace=True)
    return output_df


def add_close_price_one_day_template(df, date,
                                     wind_code_col_name='wind_code',
                                     close_price_col_name='close_price',
                                     fields='*',
                                     func_get_price=get_eod_price_one_day):
    assert isinstance(df, pd.DataFrame)
    wind_code_list = df[wind_code_col_name].to_list()
    price_df = func_get_price(wind_codes=wind_code_list,
                              date=date,
                              fields=fields)
    price_dict = price_df.set_index('S_INFO_WINDCODE')['S_DQ_CLOSE'].to_dict()
    if close_price_col_name not in df.columns:
        df[close_price_col_name] = None
    for wind_code in price_dict.keys():
        df.loc[df[wind_code_col_name] == wind_code, close_price_col_name] = price_dict[wind_code]
    return df


def add_close_price_one_day(df, date,
                            wind_code_col_name='wind_code',
                            close_price_col_name='close_price',
                            fields='*'):
    df = add_close_price_one_day_template(df, date,
                                          wind_code_col_name=wind_code_col_name,
                                          close_price_col_name=close_price_col_name,
                                          fields=fields,
                                          func_get_price=get_eod_price_one_day)
    df = add_close_price_one_day_template(df, date,
                                          wind_code_col_name=wind_code_col_name,
                                          close_price_col_name=close_price_col_name,
                                          fields=fields,
                                          func_get_price=get_funds_nav_precise_one_day)
    idx_not_num = df[close_price_col_name].apply(lambda x: (isinstance(x, float)) or (isinstance(x, int)))
    if not idx_not_num.all():
        raise ValueError('Miss some price!!!\n{}'.format(df.loc[idx_not_num].to_string()))
    return df


def get_funds_close_price_one_day(wind_codes, date, fields='*'):
    logger.info('Get close of {} on {}'.format(wind_codes, date))
    assert isinstance(wind_codes, list)
    date_str = convert_2_date_str(date)
    wind_code_str_list = str_join_under_1000(wind_codes)
    tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
    sql = "select {} from wind_filesync.ChinaClosedFundEODPrice where (S_INFO_WINDCODE in ({})) and TRADE_DT = '{}'".format(
        fields, tmp_str, date_str)
    output_df = sql_2_pddf(sql=sql)
        # logger.info('spl = \n{}'.format(sql))
        # CURSOR.execute(sql)
        # print(tmp_str)

    # raw_data = CURSOR.fetchall()
    #
    # assert len(raw_data) > 0, 'Error in length of raw_date: {}'.format(raw_data)
    #
    # col_names = [i[0] for i in CURSOR.description]
    # output_df = pd.DataFrame(data=raw_data, columns=col_names)
    return output_df


def get_funds_nav_precise_one_day(wind_codes, date, fields='*'):
    logger.info('Get precise nav of {} on {}'.format(wind_codes, date))

    assert isinstance(wind_codes, list)
    date_str = convert_2_date_str(get_tomorrow(date=date))
    wind_code_str_list = str_join_under_1000(wind_codes)
    tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
    sql = "select {} from wind_filesync.ChinaETFPchRedmList where (S_INFO_WINDCODE in ({})) and TRADE_DT = '{}'".format(
        fields, tmp_str, date_str)
    output_df = sql_2_pddf(sql=sql)
    output_df['S_DQ_CLOSE'] = output_df['F_INFO_MINPRASET'] / output_df['F_INFO_MINPRUNITS']
        # logger.info('spl = \n{}'.format(sql))
        # CURSOR.execute(sql)
        # print(tmp_str)

    # raw_data = CURSOR.fetchall()
    #
    # assert len(raw_data) > 0, 'Error in length of raw_date: {}'.format(raw_data)
    #
    # col_names = [i[0] for i in CURSOR.description]
    # output_df = pd.DataFrame(data=raw_data, columns=col_names)
    return output_df


def get_total_shares_one_day(wind_code, date):
    # con = con_pool.acquire()
    # cursor = con.cursor()
    assert isinstance(wind_code, str)
    if isinstance(date, dt.datetime):
        date = date.date()
    assert isinstance(date, dt.date)

    date_str = date.strftime('%Y%m%d')
    sql = "select TOT_SHR from wind_filesync.AShareCapitalization where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(wind_code, date_str)
    # cursor.execute(sql)
    # raw_data = cursor.fetchall()
    # cursor.close()
    # con_pool.release(con)
    raw_data = sql_raw(sql)

    assert len(raw_data) == 1, 'Error in length of raw_date: {}'.format(raw_data)
    assert len(raw_data[0]) == 1, 'Error in length of raw_date: {}'.format(raw_data)

    output = raw_data[0][0]
    assert isinstance(output, float)


    return output


# @lru_cache(maxsize=5000)
def get_index_close_price_one_day(wind_code, date, with_cached_series=False):
    # con = oracle_pool.acquire()
    # cursor = con.cursor()
    # assert isinstance(wind_code, str)
    #
    # date_str = convert_2_date_str(date)
    # sql = "select S_DQ_CLOSE from wind_filesync.AIndexEODPrices where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(wind_code, date_str)
    # logger.info('sql = {}'.format(sql))
    # cursor.execute(sql)
    # raw_data = cursor.fetchall()
    #
    # if not raw_data:
    #     sql = "select S_DQ_CLOSE from wind_filesync.HKIndexEODPrices where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(
    #         wind_code, date_str)
    #     logger.info('sql = {}'.format(sql))
    #     cursor.execute(sql)
    #     raw_data = cursor.fetchall()
    #     if not raw_data:
    #         sql = "select S_DQ_CLOSE from wind_filesync.GlobalIndexEOD where S_INFO_WINDCODE = '{}' and TRADE_DT = '{}'".format(
    #             wind_code, date_str)
    #         logger.info('sql = {}'.format(sql))
    #         cursor.execute(sql)
    #         raw_data = cursor.fetchall()
    #         if not raw_data:
    #             sql = "select F2_1288 from (WINDDB.TB_OBJECT_1288 inner join WINDDB.TB_OBJECT_1090 on WINDDB.TB_OBJECT_1288.F1_1288=WINDDB.TB_OBJECT_1090.F2_1090) where F3_1288 = '{}' and F16_1090 = '{}'".format(
    #                 date_str, wind_code.split('.')[0])
    #             logger.info('sql = {}'.format(sql))
    #             cursor.execute(sql)
    #             raw_data = cursor.fetchall()
    # cursor.close()
    # oracle_pool.release(con)
    # assert len(raw_data) == 1, 'Error in length of raw_date: {}'.format(raw_data)
    # assert len(raw_data[0]) == 1, 'Error in length of raw_date: {}'.format(raw_data)
    #
    # output = raw_data[0][0]
    if with_cached_series:
        df_all_time = get_index_close_price(wind_code=wind_code)
    else:
        df_all_time = get_index_close_price(wind_code=wind_code, begin_date=date, end_date=date)
    output = df_all_time.loc[convert_2_date_str(date), 'S_DQ_CLOSE']
    assert isinstance(output, float)
    # logger.info('output = {}'.format(output))
    return output


@lru_cache(maxsize=100)
def get_index_close_price(wind_code, begin_date=dt.date(1000, 1, 1), end_date=dt.date(3000, 1, 1)):
    # if end_date is None:
    #     end_date = dt.date.today()
    begin_date_str = convert_2_date_str(begin_date)
    end_date_str = convert_2_date_str(end_date)
    sql_list = []
    sql1 = "select TRADE_DT,S_DQ_CLOSE from wind_filesync.AIndexEODPrices " \
           "where S_INFO_WINDCODE = '{}' and TRADE_DT >= '{}' and TRADE_DT <= '{}'".format(
            wind_code, begin_date_str, end_date_str)
    sql_list.append(sql1)
    sql2 = "select TRADE_DT,S_DQ_CLOSE from wind_filesync.HKIndexEODPrices " \
           "where S_INFO_WINDCODE = '{}' and TRADE_DT >= '{}' and TRADE_DT <= '{}'".format(
            wind_code, begin_date_str, end_date_str)
    sql_list.append(sql2)
    sql3 = "select TRADE_DT,S_DQ_CLOSE from wind_filesync.GlobalIndexEOD " \
           "where S_INFO_WINDCODE = '{}' and TRADE_DT >= '{}' and TRADE_DT <= '{}'".format(
            wind_code, begin_date_str, end_date_str)
    sql_list.append(sql3)
    sql4 = "select F3_1288 AS TRADE_DT,F2_1288 AS S_DQ_CLOSE " \
           "from (WINDDB.TB_OBJECT_1288 inner join WINDDB.TB_OBJECT_1090 " \
           "on WINDDB.TB_OBJECT_1288.F1_1288=WINDDB.TB_OBJECT_1090.F2_1090) " \
           "where F3_1288 >= '{}' and F3_1288 <= '{}' and F16_1090 = '{}'".format(
            begin_date_str, end_date_str, wind_code.split('.')[0])
    sql_list.append(sql4)
    df_data = False
    for sql in sql_list:
        df_data = sql_2_pddf(sql)
        if not df_data.empty:
            break
    assert isinstance(df_data, pd.DataFrame), 'No data!!! wind_code = {} begin_date = {} end_date = {}'.format(wind_code, begin_date_str, end_date_str)
    df_data.set_index('TRADE_DT', inplace=True)
    return df_data



