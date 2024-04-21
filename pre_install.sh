
# 下面是生成 mysql 数据库
mysql> source  tumbler/sql/create_coin_table.sql;
mysql> source  tumbler/sql/create_factor_table.sql;
mysql> source  tumbler/sql/create_tumbler_table.sql;


# 下面是从币安 下载USDT 相关交易对的行情
cd run_tumbler/tools/day_work
nohup python3 check_klines_to_mysql.py &


# 开启实时行情记录
cd run_tumbler/alpha/record
# 下面只用这个，因为只有币安能下到几乎全量的数据
nohup python3 record_binance.py > record_binance.log &


# 开启交易程序
cd run_tumbler/alpha/trader
nohup python3 alpha_trader.py > alpha_trader.log &
'''
setting = {
        "alpha_trader_strategy": {
            "class_name": "AlphaCsiV2Strategy",
            "setting": {
                "contract_exchange": "HUOBIU",
                "class_name": "AlphaCsiV2Strategy",
                "hour_func": hour_func,
                "hour_window": 1,
                "day_func": day_func,
                "keep_num": 2,
                "support_long": True,
                "support_short": False,
                "per_trade_usdt_amount": 100
            }
        }
    }
'''
上面 per_trade_usdt_amount 是每个品种的交易USDT数量， 初始是100U


