import configparser
import os
import datetime as dt
import sys
from shutil import copyfile

from src.utils.utils import new_path


DEFAULT_DICT = {'PATH': {'DEMO_SETTING': ('test', 'just_test')},
                'WIND_RDF': {'USER_NAME': ('wind_rdf_user_name', '******'),
                             'PW': ('wind_rdf_pw', '******'),
                             'IP': ('wind_rdf_ip', '******'),
                             'PORT': ('wind_rdf_port', '******'),}}


class Config(object):
    def __init__(self):
        self.main_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     os.path.pardir, os.path.pardir, os.path.pardir)
        # self.config_dir = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), '..\\config')

        log_dir = new_path(os.path.join(self.main_dir, 'log'))
        # self.log_file = os.path.join(log_dir, '{}.log'.format(dt.datetime.today().strftime('%Y%m%d%H%M%S.f')))
        self.log_file = os.path.join(log_dir,
                                     '{}_{}.log'.format(dt.datetime.today().strftime('%Y%m%d'), '_'.join(sys.argv[1:])))
        self.output_dir = new_path(os.path.join(self.main_dir, 'output'))
        self.config_dir = new_path(os.path.join(self.main_dir, 'config'))
        self.cache_dir = new_path(os.path.join(self.main_dir, 'cache'))
        self.config_file = os.path.join(self.config_dir, 'config.ini')
        self.config = configparser.ConfigParser()

        if os.path.isfile(self.config_file):
            self.config.read(self.config_file)
        for section, value in DEFAULT_DICT.items():
            for option, data in value.items():
                arg_name, default_value = data
                self.__setattr__(arg_name, self.config.get(section=section, option=option, fallback=default_value))
        self.write_used_config()

    def write_used_config(self):
        output_file = os.path.join(self.config_dir, 'used_config.ini')
        output_config = configparser.ConfigParser()
        for section, value in DEFAULT_DICT.items():
            output_config.add_section(section=section)
            for option, data in value.items():
                arg_name, default_value = data
                output_config.set(section=section, option=option, value=self.__getattribute__(arg_name))

        with open(output_file, 'w') as f:
            output_config.write(f)

        if not os.path.isfile(self.config_file):
            copyfile(output_file, self.config_file)


config = Config()
