from logging import getLogger
from functools import lru_cache

import pandas as pd

from .util import CacheMode, convert_2_date_str, sql_raw, sql_2_pddf, trade_day_list
from ...utils.utils import date_yyyymmdd

logger = getLogger()

DEFAULT_WIND_CODE_COLUMN_NAME = 'S_INFO_WINDCODE'
DEFAULT_DATE_COLUMN_NAME = 'TRADE_DT'


def get_data_one_day_one_ticker(wind_code, date, table_name, field,
                                cache_mode=CacheMode.cache_one_month_all_fields,
                                wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                                date_column_name=DEFAULT_DATE_COLUMN_NAME,
                                additional_filter=None,
                                use_newest_date_if_error=False,
                                use_newest_date_if_nan=False,
                                log_exception_flag=True):
    """
    针对主键是S_INFO_WINDCODE和TRADE_DT的wind数据表的通用解决方案
    :param use_newest_date_if_nan:
    :param field:
    :param use_newest_date_if_error: 如果报错，就尝试使用最新数据
    :param additional_filter:
    :param wind_code:
    :param date:
    :param field_with_windcode:
    :param table_name:
    :param cache_mode:
    :param wind_code_column_name:
    :param date_column_name:
    :param log_exception_flag:
    :return:
    """
    logger.debug(f'Get {field} of {wind_code} on {date} using table {table_name}\n cache_mode = {cache_mode}')
    assert isinstance(wind_code, str)
    assert isinstance(field, str)

    cache_mode = CacheMode.turn2this(cache_mode)

    date_str = convert_2_date_str(date)
    try:
        if cache_mode == CacheMode.no_cache:
            sql = f"select " \
                  f"    {field} " \
                  f"from " \
                  f"    wind_filesync.{table_name} " \
                  f"where " \
                  f"    {wind_code_column_name} = '{wind_code}' " \
                  f"        and {date_column_name} = '{date_str}'"
            sql = add_additional_filter(additional_filter=additional_filter, sql=sql)
            raw_data = sql_raw(sql)
            assert len(raw_data) == 1, 'Error in length of raw_date: {}'.format(raw_data)
            assert len(raw_data[0]) == 1, 'Error in length of raw_date: {}'.format(raw_data)

            output = raw_data[0][0]
            # assert isinstance(output, float)
            # assert not pd.isna(output)

        elif cache_mode == CacheMode.cache_one_day:
            field_with_windcode = add_column_into_fields(field, wind_code_column_name)
            all_stock_df = get_data_one_day_all_ticker(date=date, fields=field_with_windcode, set_index_column=wind_code_column_name,
                                                       table_name=table_name,
                                                       wind_code_column_name=wind_code_column_name,
                                                       date_column_name=date_column_name,
                                                       additional_filter=additional_filter)

            output = all_stock_df.loc[wind_code, field]
            # return output
        elif cache_mode == CacheMode.cache_one_day_all_fields:
            # year_month = date_str[:6]
            # begin_date_str = year_month + '01'
            # end_date_str = year_month + '31'
            all_stock_df = get_data_all_ticker_period(begin_date=date_str,
                                                      end_date=date_str,
                                                      fields='*',
                                                      table_name=table_name,
                                                      set_index_column=(wind_code_column_name, date_column_name),
                                                      # wind_code_column_name=wind_code_column_name,
                                                      date_column_name=date_column_name,
                                                      additional_filter=additional_filter
                                                      )
            # print()
            output = all_stock_df.loc[(wind_code, date_str), field]
            # return output
        elif cache_mode == CacheMode.cache_one_month_all_fields:
            year_month = date_str[:6]
            begin_date_str = year_month + '01'
            end_date_str = year_month + '31'
            all_stock_df = get_data_all_ticker_period(begin_date=begin_date_str,
                                                      end_date=end_date_str,
                                                      fields='*',
                                                      table_name=table_name,
                                                      set_index_column=(wind_code_column_name, date_column_name),
                                                      # wind_code_column_name=wind_code_column_name,
                                                      date_column_name=date_column_name,
                                                      additional_filter=additional_filter)
            # print()
            output = all_stock_df.loc[(wind_code, date_str), field]
            # return output
        elif cache_mode == CacheMode.cache_one_year_all_fields:
            year = date_str[:4]
            begin_date_str = year + '0101'
            end_date_str = year + '1231'
            all_stock_df = get_data_all_ticker_period(begin_date=begin_date_str,
                                                      end_date=end_date_str,
                                                      fields='*',
                                                      table_name=table_name,
                                                      set_index_column=(wind_code_column_name, date_column_name),
                                                      # wind_code_column_name=wind_code_column_name,
                                                      date_column_name=date_column_name,
                                                      additional_filter=additional_filter
                                                      )
            output = all_stock_df.loc[(wind_code, date_str), field]
            # return output
        elif cache_mode == CacheMode.cache_one_ticker_all_days:
            all_stock_df = get_data_one_ticker_all_days(table_name=table_name,
                                                        wind_code=wind_code,
                                                        fields="*",
                                                        set_index_column=date_column_name,
                                                        wind_code_column_name=wind_code_column_name,
                                                        date_column_name=date_column_name,
                                                        additional_filter=additional_filter)
            output = all_stock_df.loc[date, field]
        elif cache_mode == CacheMode.cache_entire_table:
            # cache住整张表，慎用该模式
            all_stock_df = get_all_data_one_table(table_name=table_name,
                                                  set_index_column=(wind_code_column_name, date_column_name))
            output = all_stock_df.loc[(wind_code, date_str), field]
        else:
            raise NotImplementedError('Do not know this cache mode.')
        assert not pd.isna(output), f'\nget_data_one_day_one_ticker({wind_code}, {date}, {table_name}, {field},' \
                                    f'{cache_mode}, {wind_code_column_name},' \
                                    f'{date_column_name}, {additional_filter})\n' \
                                    f'return nan'
        return output
    except KeyError as e:

        if use_newest_date_if_error:
            logger.warning(f'Can not get {field} of {wind_code} on {date} from {table_name}\n'
                           f'Use newest date instead.')
            return cal_newest_data4get_data_one_day_one_ticker(additional_filter, cache_mode, date, date_column_name,
                                                               field, table_name, wind_code, wind_code_column_name)
        else:
            if log_exception_flag:
                logger.exception(f'Error when calling func get_data_one_day_one_ticker\n'
                                 f'tabel_name = {table_name}\n'
                                 f'wind_code = {wind_code}\n'
                                 f'date = {date}\n'
                                 f'field = {field}\n'
                                 f'cache_mode = {cache_mode}\n'
                                 f'wind_code_column_name = {wind_code_column_name}\n'
                                 f'date_column_name = {date_column_name}\n'
                                 f'additional_filter = {additional_filter}\n'
                                 f'use_newest_date_if_error = {use_newest_date_if_error}\n'
                                 )
            raise e

    except AssertionError as e:
        if pd.isna(output):
            if use_newest_date_if_nan:
                logger.warning(f'Get nan at {field} of {wind_code} on {date} from {table_name}\n'
                               f'Use newest date instead.')
                new_filter = f'{field} is not null'
                if additional_filter is None:
                    newest_date_additional_filter = new_filter
                else:
                    if isinstance(additional_filter, tuple):
                        newest_date_additional_filter = list(additional_filter).append(new_filter)
                        newest_date_additional_filter = tuple(newest_date_additional_filter)
                    elif isinstance(additional_filter, str):
                        newest_date_additional_filter = (additional_filter, new_filter)
                    else:
                        raise NotImplementedError(f'Unknown additional filter type {additional_filter}')
                latest_date = get_latest_date(table_name=table_name,
                                              wind_code=wind_code,
                                              date=date,
                                              wind_code_column_name=wind_code_column_name,
                                              date_column_name=date_column_name,
                                              additional_filter=newest_date_additional_filter)
                if pd.isna(latest_date):
                    logger.warning(f'No more early data!!! Return None')
                    return None
                logger.info(f'Use newest date = {latest_date} instead of {date}')
                return get_data_one_day_one_ticker(wind_code=wind_code,
                                                   date=latest_date,
                                                   field=field,
                                                   table_name=table_name,
                                                   cache_mode=cache_mode,
                                                   wind_code_column_name=wind_code_column_name,
                                                   date_column_name=date_column_name,
                                                   additional_filter=additional_filter,
                                                   use_newest_date_if_error=False)
            else:
                return output
        else:
            if log_exception_flag:
                logger.exception(f'Error when calling func get_data_one_day_one_ticker\n'
                                 f'wind_code = {wind_code}\n'
                                 f'date = {date}\n'
                                 f'field = {field}\n'
                                 f'cache_mode = {cache_mode}\n'
                                 f'wind_code_column_name = {wind_code_column_name}\n'
                                 f'date_column_name = {date_column_name}\n'
                                 f'additional_filter = {additional_filter}\n'
                                 f'use_newest_date_if_error = {use_newest_date_if_error}\n'
                                 )
            raise e
    except ValueError as e:
        if log_exception_flag:
            logger.exception(f'Strange output when calling func get_data_one_day_one_ticker\n'
                             f'wind_code = {wind_code}\n'
                             f'date = {date}\n'
                             f'table = {table_name}\n'
                             f'field = {field}\n'
                             f'cache_mode = {cache_mode}\n'
                             f'wind_code_column_name = {wind_code_column_name}\n'
                             f'date_column_name = {date_column_name}\n'
                             f'additional_filter = {additional_filter}\n'
                             f'use_newest_date_if_error = {use_newest_date_if_error}\n'
                             )
        logger.warning(f'Strange output when calling func get_data_one_day_one_ticker. '
                       f'output = {output}')
        return output


def cal_newest_data4get_data_one_day_one_ticker(additional_filter, cache_mode, date, date_column_name, field,
                                                table_name, wind_code, wind_code_column_name):
    latest_date = get_latest_date(table_name=table_name,
                                  wind_code=wind_code,
                                  date=date,
                                  wind_code_column_name=wind_code_column_name,
                                  date_column_name=date_column_name,
                                  additional_filter=additional_filter)
    if pd.isna(latest_date):
        raise ValueError(f'latest_date is Nan!!! \n'
                         f'table_name = {table_name},\n'
                         f'date = {date}\n'
                         f'wind_code_column_name = {wind_code_column_name}\n'
                         f'date_column_name = {date_column_name}\n'
                         f'additional_filter = {additional_filter}')
    logger.info(f'Use newest date = {latest_date} instead of {date}')
    return get_data_one_day_one_ticker(wind_code=wind_code,
                                       date=latest_date,
                                       field=field,
                                       table_name=table_name,
                                       cache_mode=cache_mode,
                                       wind_code_column_name=wind_code_column_name,
                                       date_column_name=date_column_name,
                                       additional_filter=additional_filter,
                                       use_newest_date_if_error=False)


def add_column_into_fields(field, column_name):
    if (field != '*') and (column_name not in field):
        field += f',{column_name}'
    return field


@lru_cache(maxsize=5000)
def get_data_one_day_all_ticker(table_name, date=None, fields='*', set_index_column=None,
                                wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                                date_column_name=DEFAULT_DATE_COLUMN_NAME,
                                additional_filter=None,):
    assert date is not None
    fields = add_column_into_fields(fields, wind_code_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of all stocks on {date} using table {table_name}')
    # assert isinstance(wind_codes, list)
    date_str = convert_2_date_str(date)
    # wind_code_str_list = str_join_under_1000(wind_codes)
    # tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
    sql = f"select " \
          f"    {fields} " \
          f"from " \
          f"    wind_filesync.{table_name} " \
          f"where " \
          f"    {date_column_name} = '{date_str}'"
    sql = add_additional_filter(additional_filter, sql)
    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        output_df.set_index(set_index_column, inplace=True)
    return output_df


@lru_cache(maxsize=10000)
def get_data_one_ticker_all_days(table_name, wind_code, fields='*', set_index_column=None,
                                 wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                                 date_column_name=DEFAULT_DATE_COLUMN_NAME,
                                 additional_filter=None,
                                ):
    # assert date is not None
    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of {wind_code} all days using table {table_name}')
    # assert isinstance(wind_codes, list)
    # date_str = convert_2_date_str(date)
    # wind_code_str_list = str_join_under_1000(wind_codes)
    # tmp_str = ") or S_INFO_WINDCODE in (".join(wind_code_str_list)
    sql = f"select " \
          f"    {fields} " \
          f"from " \
          f"    wind_filesync.{table_name} " \
          f"where " \
          f"    {wind_code_column_name} = '{wind_code}'"
    sql = add_additional_filter(additional_filter, sql)

    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        output_df.set_index(set_index_column, inplace=True)
    return output_df


def get_data_one_ticker_period(table_name, wind_code,
                               begin_date=None,
                               end_date=None,
                               fields='*', set_index_column=None,
                               # wind_code_column_name='S_INFO_WINDCODE',
                               date_column_name=DEFAULT_DATE_COLUMN_NAME,
                               wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                               cache_mode=CacheMode.cache_one_month_all_fields):
    assert begin_date is not None
    assert end_date is not None
    assert isinstance(wind_code, str)
    begin_date_str = convert_2_date_str(begin_date)
    end_date_str = convert_2_date_str(end_date)
    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)

    cache_mode = CacheMode.turn2this(cache_mode)
    if cache_mode == CacheMode.no_cache:
        sql = f"select " \
              f"    {fields} " \
              f"from " \
              f"    wind_filesync.{table_name} " \
              f"where " \
              f"    {wind_code_column_name} = '{wind_code}' " \
              f"        and {date_column_name} >= {begin_date_str} " \
              f"        and {date_column_name} <= {end_date_str}"

        output_df = sql_2_pddf(sql=sql)

    elif cache_mode == CacheMode.cache_one_ticker_all_days:
        df = get_data_one_ticker_all_days(table_name=table_name,
                                          wind_code=wind_code,
                                          fields=fields,
                                          set_index_column=None,
                                          wind_code_column_name=wind_code_column_name,
                                          date_column_name=date_column_name
                                          )
        output_df =df[(df[date_column_name] >= begin_date_str) & (df[date_column_name] <= end_date_str)]

    else:
        raise NotImplementedError(f'Unknown cache mode: {cache_mode}')

    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        output_df.set_index(set_index_column, inplace=True)
    return output_df


@lru_cache(maxsize=500)
def get_data_all_ticker_period(table_name,
                               begin_date=None,
                               end_date=None,
                               fields='*', set_index_column=None,
                               wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                               date_column_name=DEFAULT_DATE_COLUMN_NAME,
                               additional_filter=None):
    assert begin_date is not None
    assert end_date is not None
    fields = add_column_into_fields(fields, wind_code_column_name)
    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of all stocks from  {begin_date} to {end_date} using table {table_name}')
    begin_date_str = convert_2_date_str(begin_date)
    end_date_str = convert_2_date_str(end_date)

    sql = f"select " \
          f"    {fields} " \
          f"from " \
          f"    wind_filesync.{table_name} " \
          f"where " \
          f"    {date_column_name} >= '{begin_date_str}' " \
          f"        and {date_column_name} <= '{end_date_str}'"
    sql = add_additional_filter(additional_filter, sql)

    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        output_df.set_index(set_index_column, inplace=True)
    return output_df


@lru_cache(maxsize=10)
def get_all_data_one_table(table_name,
                           set_index_column=None):
    """
    一次性取出表中所有数据，谨慎使用该函数
    :param table_name:
    :param set_index_column:
    :return:
    """
    sql = f"select " \
          f"   * " \
          f"from " \
          f"    wind_filesync.{table_name} "
    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        output_df.set_index(set_index_column, inplace=True)
    return output_df


def add_additional_filter(additional_filter, sql):
    if additional_filter is not None:
        if isinstance(additional_filter, tuple):
            additional_filter_str = ' and '.join(additional_filter)
        else:
            additional_filter_str = additional_filter
        assert isinstance(additional_filter_str, str)
        sql += f' and {additional_filter_str}'
    return sql


def get_data_all_ticker_period_daily_cache(table_name,
                                           begin_date=None,
                                           end_date=None,
                                           fields='*', set_index_column=None,
                                           wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                                           date_column_name=DEFAULT_DATE_COLUMN_NAME,
                                           additional_filter=None):
    assert begin_date is not None
    assert end_date is not None

    date_list = trade_day_list(begin_date=begin_date, end_date=end_date)

    fields = add_column_into_fields(fields, wind_code_column_name)
    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of all stocks from  {begin_date} to {end_date} using table {table_name} with daily cache')

    df_list = []
    for date in date_list:
        df = get_data_one_day_all_ticker(table_name=table_name, date=date, fields=fields,
                                         wind_code_column_name=wind_code_column_name,
                                         date_column_name=date_column_name,
                                         additional_filter=additional_filter)
        df_list.append(df)
    df_total = pd.concat(df_list)
    # output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        df_total.set_index(set_index_column, inplace=True)
    return df_total


def add_set_index_column_2_fields(fields, set_index_column):
    if isinstance(set_index_column, str):
        fields = add_column_into_fields(fields, set_index_column)
    elif isinstance(set_index_column, tuple):
        for added_column in set_index_column:
            fields = add_column_into_fields(fields, added_column)
    return fields


def get_latest_date(table_name,
                    wind_code,
                    date,
                    wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                    date_column_name=DEFAULT_DATE_COLUMN_NAME,
                    additional_filter=None,
                    cache_mode=CacheMode.cache_one_ticker_all_days
                    ):
    """
    最新日期，注意不包括当天
    :param table_name:
    :param wind_code:
    :param date:
    :param wind_code_column_name:
    :param date_column_name:
    :param additional_filter:
    :param cache_mode:
    :return:
    """
    date_str = date_yyyymmdd(date)
    cache_mode = CacheMode.turn2this(cache_mode)
    # table_name = 'AShareFinancialIndicator'
    # date_column_name = 'REPORT_PERIOD'
    # fields_str = 'S_INFO_WINDCODE,S_INFO_COMPCODE,ANN_DT,REPORT_PERIOD'
    fields_str = ','.join([wind_code_column_name, date_column_name])
    if cache_mode == CacheMode.cache_one_ticker_all_days:

        df = get_data_one_ticker_all_days(table_name=table_name,
                                          wind_code=wind_code,
                                          fields=fields_str,
                                          date_column_name=date_column_name,
                                          additional_filter=additional_filter)

    elif cache_mode == CacheMode.cache_all_ticker_all_days:
        # year_month = date_str[:6]
        begin_date_str = '19000000'
        end_date_str = '30000000'
        all_stock_df = get_data_all_ticker_period(begin_date=begin_date_str,
                                                  end_date=end_date_str,
                                                  fields=fields_str,
                                                  table_name=table_name,
                                                  # set_index_column=(wind_code_column_name, date_column_name),
                                                  # wind_code_column_name=wind_code_column_name,
                                                  date_column_name=date_column_name,
                                                  additional_filter=additional_filter)
        df = all_stock_df[all_stock_df['S_INFO_WINDCODE'] == wind_code]

    else:
        raise NotImplementedError(f'Unknown cache_mode = {cache_mode}')

    latest_date = df[df[date_column_name] < date_str][date_column_name].max()
    return latest_date