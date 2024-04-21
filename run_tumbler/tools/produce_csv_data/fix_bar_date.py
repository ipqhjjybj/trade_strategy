from datetime import datetime, timedelta
import os


def minute_replace(d):
    d = d.replace(second=0, microsecond=0)
    return d


def hour_replace(d):
    d = d.replace(minute=0, second=0, microsecond=0)
    return d


def day_replace(d):
    return d


def fix_data(origin_file, to_file, func, period="min1"):
    if period == "min1":
        inc = timedelta(minutes=1)
    elif period == "hour1":
        inc = timedelta(hours=1)
    elif period == "day1":
        inc = timedelta(days=1)

    print("{} -> {}".format(origin_file, to_file))
    pre_close = 0
    compare_d = None
    f_in = open(origin_file, "r")
    f_out = open(to_file, "w")
    for line in f_in:
        arr = line.strip().split(',')
        dtime = arr[2]
        try:
            d = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
        except Exception as ex:
            d = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S.%f')

        d = func(d)
        if compare_d is None:
            compare_d = d
        else:
            compare_d = compare_d + inc
            while compare_d < d:
                new_arr = [arr[0], arr[1], compare_d] + [pre_close] * 4 + [0]
                new_arr = [str(x) for x in new_arr]
                f_out.write((','.join(new_arr)) + "\n")
                compare_d = compare_d + inc

                print("go to fix {} {} compare_d:{} d:{}".format(arr[0], period, compare_d, d))
                # break

        arr[2] = d
        pre_close = arr[-2]
        arr = [str(x) for x in arr]
        f_out.write((','.join(arr)) + "\n")

    f_in.close()
    f_out.close()


def fix_all_data(symbol, period):
    origin_file = ".tumbler/{}_{}.csv".format(symbol, period)
    if os.path.exists(origin_file):
        to_file = ".tumbler/fix_{}_{}.csv".format(symbol, period)
        if period == "min1":
            fix_data(origin_file, to_file, minute_replace, period)
        elif period == "hour1":
            fix_data(origin_file, to_file, hour_replace, period)
        elif period == "day1":
            fix_data(origin_file, to_file, day_replace, period)

        os.remove(origin_file)


def loop1():
    from tumbler.service.mysql_service import MysqlService

    mysql_service_manager = MysqlService()

    day_symbols = mysql_service_manager.get_mysql_distinct_symbol(table='kline_1day')
    for symbol in day_symbols:
        fix_all_data(symbol, "hour1")
        #ldp.get_hour_bar_from_mysql(symbol=symbol)

#loop1()
# fix_all_data("bnb_usdt", "min1")
# fix_all_data("eth_usdt", "min1")
# fix_all_data("uni_btc", "hour1")
# fix_all_data("ltc_btc", "hour1")
# fix_all_data("btc_usdt", "min1")
# fix_all_data("eth_usdt", "min1")
# fix_all_data("eth_usdt", "hour1")
fix_all_data("btc_usdt", "hour1")
# fix_all_data("ltc_usdt", "hour1")
# fix_all_data("btc_usdt", "day1")
