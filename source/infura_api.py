from web3 import Web3
# Uniswap Router v2: 0x7a250d5630b4cf539739df2c5dacb4c659f2488d
infura_url = "https://mainnet.infura.io/v3/75fab1daca4c4e638e18ff1ccf4bd976"
w3 = Web3(Web3.HTTPProvider(infura_url))
print(f"Connection Status: {w3.isConnected()}")
print('=========================================')

def get_transaction(transaction_hash:str):
    return w3.eth.get_transaction(transaction_hash)

def test():
    abi = [{"name": "NewExchange", "inputs": [{"type": "address", "name": "token", "indexed": True}, {"type": "address", "name": "exchange", "indexed": True}], "anonymous": False, "type": "event"}]
    uniswap = w3.eth.contract('0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95', abi=abi)
    events = uniswap.events.NewExchange.createFilter(fromBlock=6627917).get_all_entries()
    token_exchange = {e.args.token: e.args.exchange for e in events}

    for token, exchange in token_exchange.items():
        print(token, exchange)
    return

if __name__ == "__main__":
    transaction = get_transaction('0xa7b22a240c6ddc7b16e34ec7d7cc395134f33ad2faff6c1bd1708b5dab31e176')
    print(transaction)