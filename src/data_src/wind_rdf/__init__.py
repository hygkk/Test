import logging
from sqlalchemy.pool import NullPool

from sqlalchemy import create_engine
from src.config.config import config


logger = logging.getLogger()


ENGINE = create_engine(f"mysql+pymysql://{config.wind_rdf_user_name}:{config.wind_rdf_pw}@{config.wind_rdf_ip}:{config.wind_rdf_port}",
                       poolclass=NullPool)