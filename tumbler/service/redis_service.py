# coding=utf-8
import uuid
import time

import redis
from copy import copy
from enum import Enum

import tumbler.config as config
from tumbler.function import get_from_vt_key, get_vt_key
from tumbler.object import AccountData, ContractData, TickData
from tumbler.config import PREFIX


# redis_key:

class RedisKeyType(Enum):
    """
    redis key type
    """
    # REDIS_ORDER_BOOK = "order_book"
    REDIS_TICKER = "ticker"
    REDIS_CONTRACT = "contract"
    REDIS_ACCOUNT = "account"


# key_type:[REDIS_ORDER_BOOK , REDIS_TICKER]
def get_redis_key(key_type, exchange, unique=""):
    if unique:
        return PREFIX + key_type + "_" + exchange + "_" + unique
    else:
        return PREFIX + key_type + "_" + exchange


class RedisService(object):
    def __init__(self):
        self.pool = redis.ConnectionPool(
            host=config.SETTINGS["REDIS_SERVER"],
            port=config.SETTINGS["REDIS_PORT"],
            password=config.SETTINGS["REDIS_PASSWORD"],
            max_connections=config.SETTINGS["MAX_CONNECTIONS"]
        )

    def get_conn(self):
        r = redis.Redis(connection_pool=self.pool)
        return r

    def set_ticker_to_redis(self, tick):
        conn = redis.Redis(connection_pool=self.pool)
        conn.hset(get_redis_key(RedisKeyType.REDIS_TICKER.value, tick.exchange), tick.symbol, tick.get_json_msg())

    def set_contract_to_redis(self, contract):
        conn = redis.Redis(connection_pool=self.pool)
        conn.hset(get_redis_key(RedisKeyType.REDIS_CONTRACT.value, contract.exchange), contract.symbol,
                  contract.get_json_msg())

    def set_account_to_redis(self, account):
        conn = redis.Redis(connection_pool=self.pool)
        conn.hset(get_redis_key(RedisKeyType.REDIS_ACCOUNT.value, account.gateway_name, account.api_key),
                  account.account_id, account.get_json_msg())

    def get_tickers_from_redis(self, exchange):
        conn = redis.Redis(connection_pool=self.pool)
        dic = conn.hgetall(get_redis_key(RedisKeyType.REDIS_TICKER.value, exchange))
        ret_dic = {}
        for symbol, data in dic.items():
            tick_data = TickData()
            tick_data.get_from_json_msg(data)
            ret_dic[get_vt_key(symbol, exchange)] = copy(tick_data)
        return ret_dic

    def get_account_from_redis(self, vt_account_id, api_key=""):
        conn = redis.Redis(connection_pool=self.pool)
        exchange, currency = get_from_vt_key(vt_account_id)

        if exchange is not None:
            data = conn.hget(get_redis_key(RedisKeyType.REDIS_ACCOUNT.value, exchange, api_key), currency)

            if data is not None:
                account = AccountData()
                account.get_from_json_msg(data)
                return account
            else:
                return None
        else:
            return None

    def get_contract_from_redis(self, vt_symbol):
        conn = redis.Redis(connection_pool=self.pool)
        symbol, exchange = get_from_vt_key(vt_symbol)

        if symbol is not None:
            data = conn.hget(get_redis_key(RedisKeyType.REDIS_CONTRACT.value, exchange), symbol)

            if data is not None:
                contract = ContractData()
                contract.get_from_json_msg(data)
                return contract
            else:
                return None
        else:
            return None

    def acquire_lock(self, lock_name, acquire_time=10, time_out=10):
        conn = redis.Redis(connection_pool=self.pool)
        # 生成唯一id
        identifier = str(uuid.uuid4())
        # 客户端获取锁的结束时间
        end = time.time() + acquire_time
        # key
        lock_names = "lock_name:" + lock_name
        while time.time() < end:
            # setnx(key,value) 只有key不存在情况下，将key的值设置为value，若key存在则不做任何动作,返回True和False
            if conn.setnx(lock_names, identifier):
                # 设置键的过期时间，过期自动剔除，释放锁
                conn.expire(lock_names, time_out)
                return identifier
            # 当锁未被设置过期时间时，重新设置其过期时间
            elif conn.ttl(lock_names) == -1:
                conn.expire(lock_names, time_out)
            time.sleep(0.001)
        return None

    # 锁的释放
    def release_lock(self, lock_name, identifire):
        conn = redis.Redis(connection_pool=self.pool)
        lock_names = "lock_name:" + lock_name
        pipe = conn.pipeline(True)
        while True:
            try:
                # 通过watch命令监视某个键，当该键未被其他客户端修改值时，事务成功执行。当事务运行过程中，发现该值被其他客户端更新了值，任务失败
                pipe.watch(lock_names)
                if pipe.get(lock_names).decode() == identifire:  # 检查客户端是否仍然持有该锁
                    # multi命令用于开启一个事务，它总是返回ok
                    # multi执行之后， 客户端可以继续向服务器发送任意多条命令， 这些命令不会立即被执行， 而是被放到一个队列中， 当 EXEC 命令被调用时， 所有队列中的命令才会被执行
                    pipe.multi()
                    # 删除键，释放锁
                    pipe.delete(lock_names)
                    # execute命令负责触发并执行事务中的所有命令
                    pipe.execute()
                    return True
                pipe.unwatch()
                break
            except redis.exceptions.WatchError:
                # # 释放锁期间，有其他客户端改变了键值对，锁释放失败，进行循环
                pass
        return None


redis_service_manager = RedisService()
