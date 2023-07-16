from abc import ABC, abstractmethod
from logging import getLogger

from src.data_src.wind_rdf.util import CacheMode


logger = getLogger()


class Factor(ABC):
    name = None
    description = None
    formular = None
    factor_type = None

    def __init__(self):
        super(Factor, self).__init__()

    @abstractmethod
    def cal_factor_once(self, wind_code, date, cache_mode=CacheMode.cache_one_day_all_fields):
        raise NotImplementedError()

    def get_factor_name(self):
        return self.name

    def get_description(self):
        return self.description

    def get_formular(self):
        return self.formular

    def get_factor_type(self):
        return self.factor_type
