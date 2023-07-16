import argparse

from src.main import main

parser = argparse.ArgumentParser("在这里写程序名")

group = parser.add_mutually_exclusive_group()

# 必选功能
group.add_argument('-t', '--test', action='store_true', help='test mode')
group.add_argument('--demo_func', action='store_true',
                   help='求input的乘方')
group.add_argument('--load_factor_values', action='store_true',
                   help='获取所有因子值，主要用于测试功能')

#可选功能
parser.add_argument('--input',
                    action='store',
                    type=str,
                    help='输入变量')
parser.add_argument('--begin_date',
                    action='store',
                    type=str,
                    help='起始日期')
parser.add_argument('--end_date',
                    action='store',
                    type=str,
                    help='终止日期')
args = parser.parse_args()

if __name__ == '__main__':
    print(args)
    main(args)