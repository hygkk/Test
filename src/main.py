from .utils.log_utils import get_logger
from .config.config import config
from .utils.wrappers import time_this


logger = get_logger(config.log_file)


@time_this
def main(args):
    try:
        if args.test:
            logger.info("Testing...")
        elif args.demo_func:
            logger.info("Demo")
            from .demo.demo_func import demo_func
            x = int(args.input)
            logger.info('x ** 2 = {}'.format(demo_func(x)))
        elif args.load_factor_values:
            logger.info('Loading factor values...')
            from .load_factor_values import load_factor_values
            load_factor_values(begin_date=args.begin_date, end_date=args.end_date)
        else:
            logger.error('No this command !!!')
            raise NotImplementedError('No this command !!!')
    except Exception:
        logger.exception('Error:')
