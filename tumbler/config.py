# coding=utf-8

from enum import Enum
from logging import DEBUG

from tumbler.function import load_json

#####################################
# unique 唯一性编码

SETTINGS = {
    #####################################
    # redis config
    "REDIS_SERVER": '127.0.0.1',  # redis_server
    "REDIS_PORT": 6379,  # port
    "REDIS_PASSWORD": "8btc-quant.now",  # redis password
    "MAX_CONNECTIONS": 10,  # 最大连接数 redis_max_connections

    #####################################
    # rabbitmq config
    "UNIQUE_MQ_QUEUE_PROGRAM": "1",  # rabbit_MQ  unique_queue
    "MQ_SERVER": "127.0.0.1",  # rabbtimq_server
    "MQ_PORT": 5672,  # rabbitmq_port
    "MQ_USER": "admin",  # rabbitmq_user
    "MQ_PASSWD": "admin",  # rabbitmq_password

    "MQ_TRANSFER_EXCHANGE": "mq_transfer",
    "FANOUT": "fanout",  # fanout

    #####################################
    # mongodb config
    "mongodb_database": "tumbler",
    "mongodb_host": "127.0.0.1",
    "mongodb_port": 27017,
    "mongodb_user": "root",
    "mongodb_password": "8btc-quant.now",
    "mongodb_authentication_source": "admin",

    #####################################
    # mysql config
    "mysql_database": "tumbler",
    "mysql_host": "127.0.0.1",
    "mysql_port": 3306,
    "mysql_user": "root",
    "mysql_password": "Shabi86458043.",

    #####################################
    # log config
    "log.level": DEBUG,
    "log.console": True,
    "log.file": True,

    #####################################
    "ETH_PROVIDER": "https://mainnet.infura.io/v3/9785c3b226dd4c2e9bc9a62739059356",

    #####################################
    # ding talk
    "dingtalk_access_token": "72b66ea612ec76d5b279511c6aa3ac7d0c29dfdb8047fc3759e2858131fde616",
    "dingtalk_host": "https://oapi.dingtalk.com",
    "dingtalk_keyword": "quant",

    "dingtalk_message_access_token": "13fb285a60a185e32da7b44d907c97170cecff802f1a3a8c58730e4e7918cb5c",
    "dingtalk_message_keyword": "quant",

    "dingtalk_friend_message": "beee5a3596bd768a89011a2d4113d32572d3a1042ee11ef42ba59266a5ff1c7e",
    "dingtalk_friend_keyword": "quant",
    #####################################
    # proxy gateway
    "proxy_host": "127.0.0.1",
    "proxy_port": 1087,

    #####################################
    # mov gateway
    "mov_market_host": "https://ex.movapi.com",
    "mov_trade_host": "https://ex.movapi.com",
    # "mov_market_host": "http://161.189.9.64:3000",
    # "mov_trade_host": "http://161.189.9.64:3000",
    "mov_ws_market_host": "ws://bcapi.movapi.com/vapor/v3/websocket",
    "mov_ws_trade_host": "ws://bcapi.movapi.com/vapor/v3/websocket",

    "mov_flash_host": "http://ex.movapi.com",
    "mov_super_default_volume": 100,
    "mov_super_host": "http://bcapi.movapi.com",
    "mov_super_network": "mainnet",
    # "mov_super_host": "http://52.82.24.162:5001",
    # "mov_super_network": "testnet",

    #####################################

    #####################################
    "huobi_market_host": "https://api.huobipro.com",
    "huobi_trade_host": "https://api.huobipro.com",
    "huobi_ws_market_host": "wss://api.huobi.pro/ws",
    #"huobi_ws_trade_host": "wss://api.huobi.pro/ws/v1",
    "huobi_ws_trade_host": "wss://api.huobi.pro/ws/v2",

    #####################################
    "huobis_market_host": "https://api.hbdm.com",
    "huobis_trade_host": "https://api.hbdm.com",
    "huobis_ws_market_host": "wss://api.hbdm.com/swap-ws",
    "huobis_ws_trade_host": "wss://api.hbdm.com/swap-notification",

    #####################################
    "huobiu_market_host": "https://api.hbdm.com",
    "huobiu_trade_host": "https://api.hbdm.com",
    "huobiu_ws_market_host": "wss://api.hbdm.com/linear-swap-ws",
    "huobiu_ws_trade_host": "wss://api.hbdm.com/linear-swap-notification",

    #####################################
    "huobif_market_host": "https://api.hbdm.com",
    "huobif_trade_host": "https://api.hbdm.com",
    "huobif_ws_market_host": "wss://api.hbdm.com/ws",
    "huobif_ws_trade_host": "wss://api.hbdm.com/notification",
    
    #####################################
    "nexus_market_host": "https://nexus.kronostoken.com",
    "nexus_trade_host": "https://nexus.kronostoken.com",
    "nexus_ws_market_host": "wss://nexus.kronostoken.com/ws/{}/{}",
    "nexus_ws_trade_host": "wss://nexus.kronostoken.com/ws/{}/{}",
    "nexus_api_key": "4weq1nBM/gYXirCExzPkNg==",
    "nexus_secret_key": "YFFPYPMGHGMZEYH0NE820RQS2PTF",
    "nexus_account_id": "258c2557-3dea-42f7-ace0-8f9feabf7d81"
}

global_config = load_json("global_config.json")
SETTINGS.update(global_config)

#####################################
PREFIX = "8btc_"  # 巴比特



