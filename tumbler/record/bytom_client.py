# encoding: UTF-8


from tumbler.record.defi.tokens import EthNet
from tumbler.record.uniswap_client import UniswapClient


class BytomClient(UniswapClient):
    '''
    '''

    def __init__(self, address, private_key, network=EthNet.BmcTestNet.value, provider="", version=2):
        if not provider:
            if network == EthNet.BmcTestNet.value:
                # provider = "http://47.102.211.242:8545"
                provider = "http://47.102.211.242:8553"
        super(BytomClient, self).__init__(address=address, private_key=private_key,
                                          network=network, provider=provider, version=version)

    @staticmethod
    def get_bytom_client_from_keystore(keystore_file_path, password='Shabi86458043.', net=EthNet.BmcTestNet.value):
        return BytomClient.get_client_from_keystore(BytomClient, keystore_file_path, password, net)

