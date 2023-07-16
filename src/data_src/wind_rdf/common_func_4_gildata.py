from logging import getLogger
from bidict import bidict
from functools import lru_cache

from src.data_src.wind_rdf.util import CacheMode, convert_2_date_str
from .common_func_4_wind_code_date_tables import add_column_into_fields, add_set_index_column_2_fields, \
    add_additional_filter
from .util import sql_2_pddf

DEFAULT_WIND_CODE_COLUMN_NAME = 'SecuCode'
DEFAULT_DATE_COLUMN_NAME = 'TradingDay'


logger = getLogger()

MARKET_CODE_NUM_DICT = bidict({'SH': 83, 'SZ': 90, 'BJ': 18,
                               'WI': 84})


def gildata_one_day_one_ticker(wind_code,
                               date,
                               table_name,
                               field,
                               cache_mode=CacheMode.cache_one_month_all_fields,
                               wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                               table_join_col_name='InnerCode',
                               secu_main_join_col_name='InnerCode',
                               date_column_name=DEFAULT_DATE_COLUMN_NAME,
                               additional_filter=None,
                               wind_codes_used_4_cache=None,
                               use_newest_date_if_error=False,
                               use_newest_date_if_nan=False,
                               log_exception_flag=True):
    logger.debug(f'Get {field} of {wind_code} on {date} using table {table_name} with gildata\n cache_mode = {cache_mode}')
    assert isinstance(wind_code, str)
    assert isinstance(field, str)

    cache_mode = CacheMode.turn2this(cache_mode)

    date_str = convert_2_date_str(date)
    wind_code_split = wind_code.split('.')
    code = wind_code_split[0]
    market_code = wind_code_split[1]
    market_num = MARKET_CODE_NUM_DICT[market_code]
    try:
        if cache_mode == CacheMode.no_cache:
            sql = f'SELECT {field} FROM gildata.{table_name} INNER JOIN gildata.SecuMain ON gildata.{table_name}.{table_join_col_name} = gildata.SecuMain.{secu_main_join_col_name} ' \
                  f'WHERE gildata.SecuMain.SecuCode = "{code}" and {date_column_name} = "{date_str}" and gildata.SecuMain.SecuMarket = {market_num}'
            df = sql_2_pddf(sql)
            assert df.shape == (1, 1), f'Error in output shape: {df}'
            return df.values[0][0]
        elif cache_mode == CacheMode.cache_one_day_all_fields:
            # year_month = date_str[:6]
            # begin_date_str = year_month + '01'
            # end_date_str = year_month + '31'
            all_stock_df = gildata_all_ticker_period(begin_date=date_str,
                                                     end_date=date_str,
                                                     fields='*',
                                                     table_name=table_name,
                                                     table_join_col_name=table_join_col_name,
                                                     secu_main_join_col_name=secu_main_join_col_name,
                                                     set_index_column=(wind_code_column_name, date_column_name, 'SecuMarket'),
                                                     # wind_code_column_name=wind_code_column_name,
                                                     date_column_name=date_column_name,
                                                     additional_filter=additional_filter)
            return all_stock_df.loc[(code, date_str, market_num), field]

        elif cache_mode == CacheMode.cache_one_month_all_fields:
            year_month = date_str[:6]
            begin_date_str = year_month + '01'
            end_date_str = year_month + '31'
            all_stock_df = gildata_all_ticker_period(begin_date=begin_date_str,
                                                     end_date=end_date_str,
                                                     fields='*',
                                                     table_name=table_name,
                                                     set_index_column=(wind_code_column_name, date_column_name, 'SecuMarket'),
                                                     # wind_code_column_name=wind_code_column_name,
                                                     date_column_name=date_column_name,
                                                     additional_filter=additional_filter)
            return all_stock_df.loc[(code, date_str, market_num), field]

        elif cache_mode == CacheMode.cache_one_ticker_all_days:
            all_stock_df = gildata_one_ticker_all_days(table_name=table_name,
                                                       wind_code=wind_code,
                                                       fields="*",
                                                       set_index_column=date_column_name,
                                                       table_join_col_name=table_join_col_name,
                                                       secu_main_join_col_name=secu_main_join_col_name,
                                                       wind_code_column_name=wind_code_column_name,
                                                       date_column_name=date_column_name,
                                                       additional_filter=additional_filter)
            return all_stock_df.loc[date, field]

        elif cache_mode == CacheMode.cache_tickers_all_days:
            assert wind_code in wind_codes_used_4_cache
            all_stock_df = gildata_tickers_all_days(table_name=table_name,
                                                    wind_codes=wind_codes_used_4_cache,
                                                    fields='*',
                                                    set_index_column=(wind_code_column_name, date_column_name, 'SecuMarket'),
                                                    table_join_col_name=table_join_col_name,
                                                    secu_main_join_col_name=secu_main_join_col_name,
                                                    wind_code_column_name=wind_code_column_name,
                                                    date_column_name=date_column_name,
                                                    additional_filter=additional_filter, )
            return all_stock_df.loc[(code, date_str, market_num), field]
        else:
            raise NotImplementedError('Do not know this cache mode.')

    except Exception as e:
        if log_exception_flag:
            logger.exception(f'Exception logged!')
        raise e


@lru_cache(maxsize=500)
def gildata_all_ticker_period(table_name,
                              begin_date=None,
                              end_date=None,
                              fields='*', set_index_column=None,
                              table_join_col_name='InnerCode',
                              secu_main_join_col_name='InnerCode',
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

    sql = f'SELECT {fields} FROM gildata.{table_name} INNER JOIN gildata.SecuMain ON gildata.{table_name}.{table_join_col_name} = gildata.SecuMain.{secu_main_join_col_name} ' \
                  f'WHERE {date_column_name} >= "{begin_date_str}" and {date_column_name} <= "{end_date_str}"'
    sql = add_additional_filter(additional_filter, sql)
    df = sql_2_pddf(sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        df.set_index(set_index_column, inplace=True)
    return df


@lru_cache(maxsize=200)
def gildata_one_ticker_all_days(table_name, wind_code, fields='*', set_index_column=None,
                                table_join_col_name='InnerCode',
                                secu_main_join_col_name='InnerCode',
                                wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                                date_column_name=DEFAULT_DATE_COLUMN_NAME,
                                additional_filter=None, ):
    # date_str = convert_2_date_str(date)
    wind_code_split = wind_code.split('.')
    code = wind_code_split[0]
    market_code = wind_code_split[1]
    market_num = MARKET_CODE_NUM_DICT[market_code]

    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of {wind_code} all days using table {table_name}')

    sql = f'SELECT {fields} FROM gildata.{table_name} INNER JOIN gildata.SecuMain ON gildata.{table_name}.{table_join_col_name} = gildata.SecuMain.{secu_main_join_col_name} ' \
          f'WHERE gildata.SecuMain.SecuCode = "{code}" and gildata.SecuMain.SecuMarket = {market_num}'
    sql = add_additional_filter(additional_filter, sql)
    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        output_df.set_index(set_index_column, inplace=True)
    return output_df


@lru_cache(maxsize=10)
def gildata_tickers_all_days(table_name, wind_codes, fields='*', set_index_column=None,
                             table_join_col_name='InnerCode',
                             secu_main_join_col_name='InnerCode',
                             wind_code_column_name=DEFAULT_WIND_CODE_COLUMN_NAME,
                             date_column_name=DEFAULT_DATE_COLUMN_NAME,
                             additional_filter=None, ):
    # date_str = convert_2_date_str(date)
    codes = set()
    market_num_set = set()
    for wind_code in wind_codes:
        wind_code_split = wind_code.split('.')
        code = wind_code_split[0]
        market_code = wind_code_split[1]
        market_num = MARKET_CODE_NUM_DICT[market_code]
        codes.add(code)
        market_num_set.add(str(market_num))

    codes_str = '","'.join(codes)
    market_num_str = ','.join(market_num_set)

    fields = add_column_into_fields(fields, date_column_name)
    fields = add_set_index_column_2_fields(fields, set_index_column)
    logger.info(f'Get data of {wind_codes} all days using table {table_name}')

    sql = f'SELECT {fields} FROM gildata.{table_name} INNER JOIN gildata.SecuMain ON gildata.{table_name}.{table_join_col_name} = gildata.SecuMain.{secu_main_join_col_name} ' \
          f'WHERE gildata.SecuMain.SecuCode in ("{codes_str}") and gildata.SecuMain.SecuMarket in ({market_num_str})'
    sql = add_additional_filter(additional_filter, sql)
    output_df = sql_2_pddf(sql=sql)
    if set_index_column is not None:
        if isinstance(set_index_column, tuple):
            set_index_column = list(set_index_column)
        output_df.set_index(set_index_column, inplace=True)
    return output_df