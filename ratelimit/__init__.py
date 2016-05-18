# The MIT License (MIT)
#
# Copyright (c) 2016 Julian Bayardo
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import functools
import time
import math
import threading

import redis


class RateLimitFailureException(LookupError):
    pass


class RateLimitAttemptsException(RuntimeWarning):
    pass


class RateLimitException(RuntimeWarning):
    pass


class RateLimit(object):
    def __init__(self, database=None, prefix: str = '', sleep=time.sleep, max_retries=math.inf):
        self.redis = database
        self.prefix = prefix
        self.sleep = sleep
        self.max_retries = max_retries

        if self.max_retries < 1:
            raise ValueError('Maximum number of retries must be at least 1')

        self.locals = {}
        self.lock = threading.Lock()

    def guard(self, times: int, seconds: float, blocking: bool = True, max_retries: int = None) -> object:
        if not max_retries:
            max_retries = self.max_retries

        if max_retries < 1:
            raise ValueError('Maximum number of retries must be at least 1')

        if times < 1:
            raise ValueError('Number of times to limit per period must be at least 1')

        if seconds <= 0.0:
            raise ValueError('Units of time per limit period must be greater than 0')

        def decorator(function):
            lock = threading.Lock()
            cv = threading.Condition(lock)

            runs = 0
            reset = time.perf_counter()

            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                nonlocal times, seconds, blocking, max_retries
                nonlocal lock, cv, runs, reset

                with cv:
                    # This handles the case in which we enter the function after a long time, takes care of resetting
                    # the counter.
                    current = time.perf_counter()

                    if current - reset < seconds:
                        # We are in the normal case
                        attempts = 0

                        while runs >= times and attempts < max_retries:
                            current = time.perf_counter()

                            if current - reset >= seconds:
                                reset = current
                                runs = 0
                            elif blocking:
                                cv.wait(timeout=((reset + seconds - current)/2.0))
                            else:
                                # If we got here then the call was non blocking
                                raise RateLimitException('Non-blocking rate limit triggered', blocking)

                            attempts += 1

                        if attempts >= max_retries:
                            raise RateLimitAttemptsException('Attempts to run exceeded', attempts, max_retries)
                    else:
                        # It has been a long time since the function last run, so just reset everything.
                        reset = current
                        runs = 0

                    runs += 1

                return function(*args, **kwargs)

            return wrapper

        return decorator

    def local(self, times: int, seconds: float, key: str = None, blocking: bool = True, max_retries: int = None) -> object:
        if not max_retries:
            max_retries = self.max_retries

        if max_retries < 1:
            raise ValueError('Maximum number of retries must be at least 1')

        if times < 1:
            raise ValueError('Number of times to limit per period must be at least 1')

        if seconds <= 0.0:
            raise ValueError('Units of time per limit period must be greater than 0')

        def decorator(function):
            nonlocal key
            nonlocal self

            if not key:
                key = function.__name__

            if self.prefix != '':
                key = self.prefix + key

            # The only difference with the guard is that in this case, we keep everything in a shared dict. We can lock
            # it globally (in order to create a missing key if it were the case), or per key (using the lock we just
            # have inside it). The agreement is that no one changes the dict inside the key unless they hold the lock.
            with self.lock:
                try:
                    self.locals[key]
                except KeyError:
                    self.locals[key] = {
                        'runs': 0,
                        'reset': time.perf_counter(),
                        'lock': threading.Lock()
                    }

                    self.locals[key]['cv'] =  threading.Condition(self.locals[key]['lock'])

            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                nonlocal times, seconds, key, blocking, max_retries
                nonlocal self

                # For ease of understanding
                shared = self.locals[key]

                with shared['cv']:
                    # This handles the case in which we enter the function after a long time, takes care of resetting
                    # the counter.
                    current = time.perf_counter()

                    if current - shared['reset'] < seconds:
                        # We are in the normal case
                        attempts = 0

                        while shared['runs'] >= times and attempts < max_retries:
                            current = time.perf_counter()

                            if current - shared['reset'] >= seconds:
                                shared['reset'] = current
                                shared['runs'] = 0
                            elif blocking:
                                shared['cv'].wait(timeout=((shared['reset'] + seconds - current)/2.0))
                            else:
                                # If we got here then the call was non blocking
                                raise RateLimitException('Non-blocking rate limit triggered', blocking)

                            attempts += 1

                        if attempts > max_retries:
                            raise RateLimitAttemptsException('Attempts to run exceeded', attempts, max_retries)
                    else:
                        # It has been a long time since the function last run, so just reset everything.
                        shared['reset'] = current
                        shared['runs'] = 0

                    shared['runs'] += 1

                return function(*args, **kwargs)

            return wrapper

        return decorator

    def shared(self, times: int, seconds: int, key: str = None, blocking: bool = True, recover: bool = True, max_retries: int = None) -> object:
        if not self.redis:
            raise ValueError('Shared rate limits require a database')

        if not max_retries:
            max_retries = self.max_retries

        if max_retries < 1:
            raise ValueError('Maximum number of retries must be at least 1')

        if times < 1:
            raise ValueError('Number of times to limit must be at least 1')

        if seconds < 1:
            raise ValueError('Units of time per limit period must be at least 1')

        def decorator(function):
            nonlocal key
            nonlocal self

            if not key:
                key = function.__name__

            if self.prefix != '':
                key = self.prefix + key

            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                nonlocal times, seconds, key, blocking, recover, max_retries
                nonlocal self

                ttl = -1
                attempts = 0
                execute = True
                retry = True

                while retry and attempts < max_retries:
                    execute = True

                    try:
                        # Using this guard ensures that we destroy the transaction afterwards
                        with self.redis.pipeline(transaction=True) as pipe:
                            # Put pipeline into immediate execution mode
                            pipe.watch(key)
                            # Fetch the current value
                            value = pipe.get(key)

                            if value:
                                if value < times:
                                    # We had the value already and there are attempts left. Just increment the counter
                                    pipe.incr(key, 1)
                                else:
                                    # We are in rate limiting, fetch how long we should wait and do so
                                    ttl = pipe.ttl(key)
                                    execute = False
                            else:
                                # Put back into batch mode
                                pipe.multi()
                                # There is no counter (either it never existed, or it expired). Set the counter to 1 and
                                # set the expiration as desired
                                pipe.incr(key, 1)
                                pipe.expire(key, seconds)

                            # Execute the batch of commands in the server if there was any need to
                            pipe.execute()

                        # If we did not execute, then it was because we are in rate limiting mode
                        if not execute:
                            retry = blocking

                            # If we are allowed to retry, then just call the sleep function before entering the next
                            # iteration
                            if blocking:
                                self.sleep(ttl)
                    except redis.WatchError as error:
                        # Someone might have changed the variable while we were doing our magic. However, since all
                        # operations are atomic, this must have happened between the get and whatever comes next, so we
                        # haven't really done anything in our database
                        execute = False

                        # Just retry without sleeping (because this was due to a race condition)
                        retry = recover

                        if not recover:
                            raise RateLimitFailureException('Race condition avoided in redis, retry not allowed',
                                                            key) from error

                    # Add another attempt
                    attempts += 1

                if execute:
                    return function(*args, **kwargs)

                # After all, we couldn't execute, just raise an exception
                if not blocking:
                    # If we got here then the call was non blocking
                    raise RateLimitException('Non-blocking rate limit triggered', blocking, key)

                # If we got here then we exceeded the number of attempts
                raise RateLimitAttemptsException('Maximum number of attempts exceeded', attempts, max_retries, key)

            return wrapper

        return decorator
