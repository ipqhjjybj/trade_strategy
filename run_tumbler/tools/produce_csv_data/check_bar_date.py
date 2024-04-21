from datetime import datetime, timedelta


def check_datetime(filename, interval=60):
    pre_time = None
    ret = []
    f = open(filename, "r")
    for line in f:
        try:
            dtime = (line.strip().split(","))[2]
            d = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            if pre_time is None or d - pre_time == timedelta(minutes=1):
                pass
            else:
                print("not ok, {},{},{}".format(pre_time, d, d - pre_time))

            pre_time = d
        except Exception as ex:
            print(ex, line)

    f.close()
    return ret


check_datetime(".tumbler/btc_usdt_min1.csv", 60)
