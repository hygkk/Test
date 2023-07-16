from functools import wraps
import logging
import pandas as pd


logger = logging.getLogger()


def cut_list(func):
    '''
    数据库输入的数组函数每次只能调1000个，如果长于1000就要分段
    :param func:
    :return:
    '''
    @wraps(func)
    def wrapper(wind_codes: list, *args, **kwargs):
        # logger.info('wrapper!!!')

        if isinstance(wind_codes, list):
            if len(wind_codes) > 999:
                wind_codes_copy = wind_codes[:]
                wind_codes_list_of_list = []
                while len(wind_codes_copy) > 999:
                    wind_codes_list_of_list.append(wind_codes_copy[:999])
                    wind_codes_copy = wind_codes_copy[999:]
                logger.info('list of list:\n{}'.format(wind_codes_list_of_list))
                output_list = []
                for input_list in wind_codes_list_of_list:
                    output_list.append(func(input_list, *args, **kwargs))
                return pd.concat(output_list)
            else:
                return func(wind_codes, *args, **kwargs)
        elif isinstance(wind_codes, str):
            return func(wind_codes, *args, **kwargs)
        else:
            raise TypeError('Can not handle this type of input !!! windcodes={}'.format(wind_codes))
    return wrapper
