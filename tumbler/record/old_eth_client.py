# encoding: UTF-8
'''
import os
import json

import eth_account
from web3 import Web3, HTTPProvider, middleware
from web3.auto import w3
from web3.gas_strategies.time_based import fast_gas_price_strategy

from tumbler.record.defi.tokens import EthNet, get_symbol_2_contract_dict, get_symbol_2_decimal_dict
from tumbler.record.defi.tokens import get_main_symbol_from_net
from tumbler.record.defi.types import AddressLike
from tumbler.record.defi.util import load_contract_erc20, addr_to_str
from tumbler.record.defi.constants import ETH_ADDRESS
from tumbler.service import log_service_manager
from .base_client import BaseClient

# bytom 跨链相关配置
CROSS_ETH_RECEIVER_ADDRESS = "0x7303dd1F1E2e494b0D7F6Df3C6ACC113380978c0"
CROSS_USDT_ERC20_RECEIVER_ADDRESS = "0x840edae86e2Bf6cAA13CcbDEbbDDc8f153064087"
CROSS_USDC_RECEIVER_ADDRESS = "0x7e87c78089b92d354cc31104152ef5385fcc30e8"
CROSS_DAI_RECEIVER_ADDRESS = "0xc9390c509ebaac49e89581fe725e55c3d5e9ab14"


class EthClient(BaseClient):
    
    web3.eth.estimate_gas({'to': '0xd3CdA913deB6f67967B99D67aCDFa1712C293601', 'from':web3.eth.coinbase, 'value': 12345})


    def __init__(self, _apikey, _secret_key, net=EthNet.MainNet.value):
        super(EthClient, self).__init__(_apikey, _secret_key)
        self.address = eth_account.Account.from_key(self.secret_key).address
        self.net = net
        self.main_symbol = get_main_symbol_from_net(self.net)

        self.web3 = Web3(HTTPProvider("https://mainnet.infura.io/v3/9785c3b226dd4c2e9bc9a62739059356"))
        self.web3.middleware_onion.add(middleware.time_based_cache_middleware)
        self.web3.middleware_onion.add(middleware.latest_block_based_cache_middleware)
        self.web3.middleware_onion.add(middleware.simple_cache_middleware)
        self.web3.eth.setGasPriceStrategy(fast_gas_price_strategy)

    def transfer_eth(self, to_address, value):
        nonce = self.web3.eth.getTransactionCount(Web3.toChecksumAddress(self.address))
        transaction = {
            'value': Web3.toWei(value, 'ether'),
            'gas': 25200,
            'gasPrice': Web3.toWei('16.15', 'gwei'),
            'nonce': nonce,
            'chainId': 1,
            'to': to_address,
        }
        signed = self.web3.eth.account.signTransaction(
            transaction,
            self.secret_key
        )
        result_hash = self.web3.toHex(self.web3.eth.sendRawTransaction(signed.rawTransaction))
        print("-" * 8, "send transaction success! Hash ----  ", result_hash, '-' * 8)
        return result_hash

    def get_eth_balance(self):
        value = self.web3.eth.getBalance(self.address, 'latest')
        balance = value * 1.0 / Web3.toWei(1, 'ether')
        return balance

    def cross_eth(self, amount):
        return self.transfer_eth(CROSS_ETH_RECEIVER_ADDRESS, amount)

    def cross_usdt(self, amount):
        return self.transfer_token(CROSS_USDT_ERC20_RECEIVER_ADDRESS, amount, "USDT")

    def transfer_token(self, to_address, amount, token_symbol):
        if token_symbol.upper() == self.main_symbol:
            return self.transfer_eth(to_address, amount)
        else:
            amount = amount * (10 ** get_symbol_2_decimal_dict(token_symbol.upper()))
            token_address = Web3.toChecksumAddress(get_symbol_2_contract_dict(self.net)[token_symbol.upper()])
            erc20_contract = load_contract_erc20(self.web3, token_address)
            nonce = self.web3.eth.getTransactionCount(self.address, "pending")
            unicorn_txn = erc20_contract.functions.transfer(
                Web3.toChecksumAddress(to_address),
                amount,
            ).buildTransaction({
                'chainId': 1,
                'gas': 60000,
                'gasPrice': self.get_gas_price(),
                'nonce': nonce,
            })
            signed_txn = self.web3.eth.account.signTransaction(unicorn_txn, self.secret_key)
            result_hash = self.web3.toJSON(self.web3.eth.sendRawTransaction(signed_txn.rawTransaction))
            print("-" * 8, "send transaction success! Hash ----  ", result_hash, '-' * 8)
            return result_hash

    def get_token_balance(self, token_symbol):
        token_address = Web3.toChecksumAddress(get_symbol_2_contract_dict(self.net)[token_symbol.upper()])
        balance = self.get_token_balance_from_address(token_address)
        return balance / (10 ** get_symbol_2_decimal_dict(token_symbol.upper()))

    def get_token_balance_from_address(self, token: AddressLike):
        if addr_to_str(token) == ETH_ADDRESS:
            return self.get_eth_balance()
        erc20 = load_contract_erc20(self.web3, token)
        balance: int = erc20.functions.balanceOf(self.address).call()
        return balance

    def get_gas_price(self):
        return self.web3.eth.generateGasPrice()


    AttributeDict({'blockHash': HexBytes('0x3a3263c953d968d1bf5bd1819e06c39c3b35ead3f62f0011bcadee3569e72029'), 'blockNumber': 12955975, 'from': '0x0a3D9a7221e2E2BD5E68C5ff592bF27DEA642Af1', 'gas': 54004, 'gasPrice': 25000000000, 'hash': HexBytes('0x7503996cbe1045d164b1b8115bea2d0a30b4e13632e4b391f10d6d5889746fe3'), 'input': '0x2e1a7d4d00000000000000000000000000000000000000000000000000d529ae9e860000', 'nonce': 15, 'r': HexBytes('0x08204ff6cc20566c24eaddfe8bbb2ce6239a57fd0ccb67cbd467c5caf80859c1'), 's': HexBytes('0x4b0f2e72f6e52a02f7d311431b76ca6dde09c5913a1ea305a4f7d68119528c95'), 'to': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'transactionIndex': 8, 'type': '0x0', 'v': 37, 'value': 0})
    AttributeDict({'blockHash': None, 'blockNumber': None, 'from': '0x5791A402700e7266Bb4Ec99E508fA25a05c762f0', 'gas': 69168, 'gasPrice': 23000000000, 'hash': HexBytes('0xb65c13fd363ea94dcf19eb3640479bbc3c219eb0add30217b2e5a7e35da78842'), 'input': '0x095ea7b300000000000000000000000065383abd40f9f831018df243287f7ae3612c62ac00000000000000000000000000000000000000000005aa3e9e1dac3156d72b20', 'nonce': 485, 'r': HexBytes('0x5263e55b5bc04ebe97540f8a4740ab19b3974e3392c2f270dd298260204603d4'), 's': HexBytes('0x3a471caea0587fcd45bba38b19352e495a89d82f70290f589cf923de6603311e'), 'to': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'transactionIndex': None, 'type': '0x0', 'v': 38, 'value': 0})

    失败的tx
    AttributeDict({'blockHash': HexBytes('0x312df2a453f14f6e1fdd4bf938abf70ef8ec35841451db80efeee7e619f60ee9'), 'blockNumber': 4891840, 'from': '0x18C91698658E510837CB483BB21a70B19E087c8B', 'gas': 90000, 'gasPrice': 130000000000, 'hash': HexBytes('0x0ed9f4f76312a7141975d29f40e042fecc8a9bc7f380e3a45156ddca7590750b'), 'input': '0x', 'nonce': 0, 'r': HexBytes('0xd080ad5e77ccda6b9d72c40e318f3f15ebf023200d17c20629957a1c75cc449c'), 's': HexBytes('0x47fb7f03395fc73736f5825e740f31116a5d7d4feda984b38da502b74a010d24'), 'to': '0xEDab973D01970f6F83962FE3AaaE818F1Cd13487', 'transactionIndex': 41, 'type': '0x0', 'v': 37, 'value': 100000000000000000})

    发现区分不出来失败的tx

    def get_transaction(self, tx_id):
        transaction_info = self.web3.eth.get_transaction(tx_id)
        return transaction_info


    失败交易
    AttributeDict({'blockHash': HexBytes('0x312df2a453f14f6e1fdd4bf938abf70ef8ec35841451db80efeee7e619f60ee9'), 'blockNumber': 4891840, 'contractAddress': None, 'cumulativeGasUsed': 882334, 'effectiveGasPrice': '0x1e449a9400', 'from': '0x18C91698658E510837CB483BB21a70B19E087c8B', 'gasUsed': 21334, 'logs': [], 'logsBloom': HexBytes('0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'status': 0, 'to': '0xEDab973D01970f6F83962FE3AaaE818F1Cd13487', 'transactionHash': HexBytes('0x0ed9f4f76312a7141975d29f40e042fecc8a9bc7f380e3a45156ddca7590750b'), 'transactionIndex': 41, 'type': '0x0'})
    成功交易
    AttributeDict({'blockHash': HexBytes('0xb3a8092f10edee46d37726613e9bd3630fe6a17da5e9188ac6d7c23c48c0a1c7'), 'blockNumber': 12955985, 'contractAddress': None, 'cumulativeGasUsed': 14612624, 'effectiveGasPrice': '0x55ae82600', 'from': '0x5791A402700e7266Bb4Ec99E508fA25a05c762f0', 'gasUsed': 46112, 'logs': [AttributeDict({'address': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'blockHash': HexBytes('0xb3a8092f10edee46d37726613e9bd3630fe6a17da5e9188ac6d7c23c48c0a1c7'), 'blockNumber': 12955985, 'data': '0x00000000000000000000000000000000000000000005aa3e9e1dac3156d72b20', 'logIndex': 339, 'removed': False, 'topics': [HexBytes('0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'), HexBytes('0x0000000000000000000000005791a402700e7266bb4ec99e508fa25a05c762f0'), HexBytes('0x00000000000000000000000065383abd40f9f831018df243287f7ae3612c62ac')], 'transactionHash': HexBytes('0xb65c13fd363ea94dcf19eb3640479bbc3c219eb0add30217b2e5a7e35da78842'), 'transactionIndex': 107})], 'logsBloom': HexBytes('0x00000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000002000000080000000000042000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000020000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010200000000000000000000000000000000000000000000000000000080000'), 'status': 1, 'to': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'transactionHash': HexBytes('0xb65c13fd363ea94dcf19eb3640479bbc3c219eb0add30217b2e5a7e35da78842'), 'transactionIndex': 107, 'type': '0x0'})

    能同过status 区分失败的交易
    找不到这笔交易的话，需要捕捉处理异常


    def get_transaction_receipt(self, tx_id):
        try:
            transaction_info = self.web3.eth.get_transaction_receipt(tx_id)
            return transaction_info
        except Exception as ex:
            log_service_manager.write_log(f"[get_transaction_receipt] ex:{ex}, tx_id:{tx_id}")

'''

