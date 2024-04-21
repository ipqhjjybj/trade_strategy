# encoding: UTF-8


from tumbler.record.defi.tokens import EthNet
from tumbler.record.uniswap_client import UniswapClient


class PancakeClient(UniswapClient):
    '''
    '''

    def __init__(self, address, private_key, network=EthNet.BscNet.value, provider="", version=2):
        if not provider:
            provider = "https://bsc-dataseed.binance.org"  # my key
        super(PancakeClient, self).__init__(address=address, private_key=private_key,
                                            network=network,
                                            provider=provider, version=version)

    @staticmethod
    def get_eth_client_from_keystore(keystore_file_path, password='Shabi86458043.', net=EthNet.BscNet.value):
        return PancakeClient.get_client_from_keystore(PancakeClient, keystore_file_path, password, net)
