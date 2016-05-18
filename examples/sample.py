import ratelimit

r = ratelimit.RateLimit()

@r.local(5, 1.0, 'lmao', blocking=False)
def test():
    print("This set of tasks should happen up to 5 times per second")

@r.local(5, 1.0, 'lmao', blocking=False)
def ts2():
    print("Bounded by the same key as the previous one")

@r.guard(2, 5, blocking=False)
def test2():
    print("This one should happen twice every 5 seconds")

while 1:
    try:
        test()
    except:
        pass

    try:
        ts2()
    except:
        pass

    try:
        test2()
    except:
        pass