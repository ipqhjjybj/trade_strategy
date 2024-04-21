from datetime import datetime
import time

s = lambda x: x + 3
print(s(3))


class A(object):
    def __init__(self):
        pass

    def u(self):
        def k(u):
            return u + 3

        print(k(3))


s = A()
s.u()


def func1(s):
    filename = s.split('.')[0]
    t1, t2 = filename.split('-')
    print(t1, t2)
    return int(t1) * 1000, int(t2) * 1000


func1("1611281098-1611281396.txt")


def func2(s):
    # d = datetime.strptime(s, "%Y%m%d_%H%M%S.csv")
    t = time.strptime(s, "%Y%m%d_%H%M%S.csv")
    timestamp = int(time.mktime(t))
    print(timestamp)
    return timestamp


func2("20210206_000000.csv")
