# coding=utf-8

from tumbler.service import mysql_service_manager
from tumbler.service.mysql_service import MysqlService
from tumbler.data.coinmarket_data import CoinMarket

from tumbler.data.download.download_manager import get_all_symbols


def get_chain(symbol):
    symbol = symbol.lower()
    if symbol == "btc":
        return "btc"
    else:
        return "eth"


sql_insert_fundamental = '''
INSERT INTO `coin`.`coin_fundamental`
(`id`,
`coin`,
`name`,
`chain`,
`exchange`,
`circulating_supply`,
`max_supply`,
`create_date`)
VALUES
(NULL,
'{}',
'{}',
'{}',
'{}',
'{}',
'{}',
'{}');
'''


def insert_symbol_code():
    coin = CoinMarket()

    conn = mysql_service_manager.get_conn()
    cur = conn.cursor()
    page = 100
    for i in range(30):
        data = coin.get_latested_info(limit=page, start=i * page + 1)
        for dic in data["data"]:
            u_id = dic["id"]
            symbol = dic["symbol"]
            name = dic["name"]
            name = name.replace("'", "\\'")
            slug = dic["slug"]
            date_added = dic["date_added"].replace('T', ' ').replace('Z', '')
            max_supply = dic["max_supply"]
            if max_supply is None:
                max_supply = 0
            circulating_supply = dic["circulating_supply"]
            if not circulating_supply:
                circulating_supply = 0

            sqll = "delete from `coin`.`coin_fundamental` where `coin`='{}'".format(symbol)
            try:
                cur.execute(sqll)
            except Exception as ex:
                print(ex)
            sqll = sql_insert_fundamental.format(symbol, name, get_chain(symbol), "", circulating_supply, max_supply,
                                                 date_added)
            print(sqll)
            try:
                cur.execute(sqll)
            except Exception as ex:
                print(ex)
            conn.commit()
    cur.close()
    conn.close()


def insert_symbol_pairs():
    all_symbols = get_all_symbols()
    conn = mysql_service_manager.get_conn()
    cur = conn.cursor()
    table_name = "`coin`.`symbols`"
    sqll = "truncate table {}".format(table_name)
    cur.execute(sqll)
    sqll = "insert into {}(id,symbol) values(NULL,%s)".format(table_name)
    cur.executemany(sqll, all_symbols)
    conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    mysql_service_manager = MysqlService()
    #insert_symbol_code()
    insert_symbol_pairs()
