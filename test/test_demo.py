import unittest


class TestUtils(unittest.TestCase):
    def test_demo_func(self):
        from src.demo.demo_func import demo_func
        self.assertEqual(demo_func(2), 4)
        self.assertRaises(AssertionError, demo_func, x='a')
