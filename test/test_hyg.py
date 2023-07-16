import unittest


class TestHyg(unittest.TestCase):

    def test_gildata_one_day_one_ticker(self):
        try:
            from src.data_src.wind_rdf.common_func_4_gildata import gildata_one_day_one_ticker
            output = gildata_one_day_one_ticker('000016.SH', '20200309', table_name='QT_IndexQuote', field='ClosePrice')
            self.assertAlmostEqual(output, 2868.8297)
        except:
            self.fail()