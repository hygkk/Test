from cachier import cachier
from functools import partial
import datetime as dt

from src.config.config import config

my_cachier = partial(cachier, cache_dir=config.cache_dir, stale_after=dt.timedelta(days=1))

