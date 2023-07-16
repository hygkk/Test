import unittest


class TestFactor(unittest.TestCase):

    def test_close_factor(self):
        try:
            from src.factor_lib.daily_price.daily_price_factors import CLOSE_FACTOR
            from src.data_src.wind_rdf.util import CacheMode
            output = CLOSE_FACTOR.cal_factor_once(wind_code='600000.SH', date='20230413', cache_mode=CacheMode.cache_one_day_all_fields)
            self.assertAlmostEqual(output, 107.77)
        except:
            self.fail()

    def test_load_factor_values(self):
        try:
            from src.load_factor_values import load_factor_values
            df = load_factor_values(begin_date='20230410', end_date='20230414')
            print(df)
        except:
            self.fail()