# encoding: UTF-8

from web3.auto import w3
from web3 import Web3, HTTPProvider, middleware
from web3.gas_strategies.time_based import fast_gas_price_strategy
import eth_account

from .base_client import BaseClient

'''
bsc 开发者文档地址
https://docs.binance.org/smart-chain/developer/rpc.html

pancake 合约信息
https://docs.pancakeswap.finance/code/smart-contracts#farms-contracts

'''

pancake_factory_contract_address = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
pancake_router_contract_address = "0x10ED43C718714eb63d5aA57B78B54704E256024E"


class BscClient(BaseClient):
    def __init__(self, _apikey, _secret_key):
        super(BscClient, self).__init__(_apikey, _secret_key)

        self.sender_address = eth_account.Account.from_key(self.secret_key).address

        self.web3 = Web3(HTTPProvider("https://bsc-dataseed.binance.org/"))
        self.web3.middleware_onion.add(middleware.time_based_cache_middleware)
        self.web3.middleware_onion.add(middleware.latest_block_based_cache_middleware)
        self.web3.middleware_onion.add(middleware.simple_cache_middleware)
        self.web3.eth.setGasPriceStrategy(fast_gas_price_strategy)

    @staticmethod
    def get_bsc_client_from_keystore(keystore_file_path, password='Shabi86458043.'):
        with open(keystore_file_path) as keyfile:
            encrypted_key = keyfile.read()
            private_key = w3.eth.account.decrypt(encrypted_key, password)
            address = eth_account.Account.from_key(private_key).address
            return BscClient(address, private_key)





