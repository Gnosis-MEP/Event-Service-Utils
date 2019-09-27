import functools
import time


def timer_logger(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        logger = args[0].logger
        logger.debug(f"Finished {func.__qualname__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer
