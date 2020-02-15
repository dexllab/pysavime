import time

from pysavime.logging_utility.logger import timer_logger
import functools


def timer_decorator(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        time_start = time.time()
        r = f(*args, **kwargs)
        time_finish = time.time()
        timer_logger.info(f'It took {time_finish - time_start:.6f}s to run `{f.__qualname__}`.')
        return r

    return wrapper
