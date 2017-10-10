# from
# https://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish
# NOTE : Only works in UNIX
from functools import wraps
import errno
import os
import signal

import sys
if sys.platform.startswith('linux'):
    ON_UNIX = True
else:
    ON_UNIX = False

# TODO : for tests, test both


class TimeoutError(Exception):
    pass


if ON_UNIX:
    def timeout(seconds=None,
                error_message=os.strerror(errno.ETIME)):
        def decorator(func):
            def _handle_timeout(signum, frame):
                raise TimeoutError(error_message)

            def wrapper(*args, **kwargs):
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(seconds)
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                return result

            if seconds is None:
                return func
            else:
                return wraps(func)(wrapper)

        return decorator
else:
    # define empty decorator
    def timeout(seconds=None, error_message=None):
        def decorator(func):
            return func
        return decorator
