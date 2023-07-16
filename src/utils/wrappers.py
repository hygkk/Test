import time
from functools import wraps
from logging import getLogger


logger = getLogger()


def time_this(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info('func {} cost: {}s'.format((func.__name__), end-start))
        return result
    return wrapper


def retry(func, retry_n_times=10, sleeptime=2):
    """

    :param func:
    :param retry_n_times: 重复次数
    :param sleeptime: 每次睡眠间隔
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        error = None
        for i in range(retry_n_times):
            try:
                if i > 0:
                    logger.warning('func {} try again'.format(func.__name__))
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.exception("func {} failed {} times".format(func.__name__, i+1))
                error = e
                time.sleep(sleeptime)

        if error is not None:
            raise error
        else:
            raise Exception('You should not reach here. Check the code!!!')
    return wrapper


# 单例模式装置器
def singleton(cls, *args, **kw):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return getinstance
