# Powered by Etherscan.io APIs
import requests, os
import pandas as pd
import numpy as np
import datetime
#===================================================================
# https://etherscan.io/accounts/label/uniswap
api_key = '39M8BBF53U6M7N2YS92M163RP3RCZF6GUK'
api_key_backup = 'IGK5V2U9EW3ZJRCZHQUVGW3DEPXNVCB73G'
#===================================================================
uniswap_v2_router_address = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
# block_end = 12327654 # 2021.4.28: 12,327,654
# block_start = 10200000; # 2020.5.24: 10,127,978
#===================================================================
# https://etherscan.io/accounts/label/sushiswap
sushiswap_router_address = '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f'
block_end = 12327654 # 2021.4.28: 12,327,654
block_start = 10790000; # 2020.8.28: 10,127,978

def get_external_transaction(address:str, start_block:str, end_block:str, path:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        value_array = np.array([list(row.values()) for row in r['result']])
        df = pd.DataFrame(value_array, columns=external_all)
        df['type'] = 'external'
        df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
        df = df.set_index('timeStamp')
        df.fillna('NA')
        df.to_csv(path + str(start_block) + '.csv')
        return r['status'], 'OK'
    else:
        return r['status'], r['message']

def get_internal_transaction(address:str, start_block:str, end_block:str, path:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        value_array = np.array([list(row.values()) for row in r['result']])
        df = pd.DataFrame(value_array, columns=internal_all)
        df['type'] = 'internal'
        df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
        df = df.set_index('timeStamp')
        df.fillna('NA')
        df.to_csv(path + str(start_block) + '.csv')
        return r['status'], 'OK'
    else:
        return r['status'], r['message']


def get_ERC20_transaction(address:str, start_block:str, end_block:str, path:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        value_array = np.array([list(row.values()) for row in r['result']])
        df = pd.DataFrame(value_array, columns=erc20_all)
        df['type'] = 'erc20'
        df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
        df = df.set_index('timeStamp')
        df.fillna('NA')
        df.to_csv(path + str(start_block) + '.csv')
        return r['status'], 'OK'
    else:
        return r['status'], r['message']

def get_ERC721_transaction(address:str, start_block:str, end_block:str, path:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        value_array = np.array([list(row.values()) for row in r['result']])
        df = pd.DataFrame(value_array, columns=erc721_all)
        df['type'] = 'erc721'
        df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
        df = df.set_index('timeStamp')
        df.fillna('NA')
        df.to_csv(path + str(start_block) + '.csv')
        return r['status'], 'OK'
    else:
        return r['status'], r['message']

# def get_balance(address:str):
    # api_link = f"https://api.etherscan.io/api?module=account&action=balance&address={address}&tag=latest&apikey={api_key}"
    # r = eval(requests.get(api_link).text)
    # if r['status'] == '1':
    #     return r['status'], r['result']
    # else:
    #     return r['status'], r['message']

# def get_mined_block(address:str):
    # api_link = f"https://api.etherscan.io/api?module=account&action=getminedblocks&address={address}&blocktype=blocks&apikey={api_key}"
    # r = eval(requests.get(api_link).text)
    # if r['status'] == '1':
    #     return r['status'], r['result']
    # else:
    #     return r['status'], r['message']

def collect(address:str, block_start:str, block_end:str, save_path:str):
    index = 0
    block_index = block_start
    error_list = []
    while block_index < block_end:
        # ===================== Change the functions here to collect different types of transactions =====================
        # ===================== Don't forget to change the save_path as well =====================
        # status, result = get_external_transaction(address, block_index, block_index+10000, save_path)
        # status, result = get_internal_transaction(address, block_index, block_index+10000, save_path)
        status, result = get_ERC20_transaction(address, block_index, block_index+10000, save_path)
        # status, result = get_ERC721_transaction(address, block_index, block_index+10000, save_path)

        if status == '1':
            print(f"complete: {block_index} to {block_index+10000}; ", result)
            # if index > 15:
            #     break
        else:
            print(f"error: {block_index} to {block_index+10000}; ", result)
            error_list.append((block_index, result))
        index += 1
        block_index += 10000
    if error_list != []:
        with open(f'./source/data/error_log/{datetime.datetime.now()}.txt', 'w') as f:
            f.write(str(error_list))
    
    return
if __name__ == '__main__':
    # ======================= Raw Attributes =======================
    external_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'confirmations']
    internal_all = ['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'value', 'contractAddress', 'input', 'type', 'gas', 'gasUsed', 'traceId', 'isError', 'errCode']
    erc20_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
    erc721_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
    # ======================= Selected Attributes =======================
    # external_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed'] # tokenName, tokenDecimal
    # erc20_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']
    # erc721_select = ['blockNumber', 'timeStamp', 'from', 'to', 'tokenID', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']
    # ======================= Start Program =======================
    # collect(uniswap_v2_router_address, block_start, block_end, './source/data/uniswap_v2/erc721/')
    collect(sushiswap_router_address, block_start, block_end, './source/data/sushiswap/erc20/')

    