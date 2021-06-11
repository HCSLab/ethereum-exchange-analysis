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
# end_block = 12327654 # 2021.4.28: 12,327,654
# start_block = 10200000; # 2020.5.24: 10,127,978
#===================================================================
uniswap_v3_router_address = '0xE592427A0AEce92De3Edee1F18E0157C05861564'
# end_block = 
# start_block = 12343421;	# 2021-04-30 18:23:59
#===================================================================
# https://etherscan.io/accounts/label/sushiswap
sushiswap_router_address = '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f'
# end_block = 12327654 # 2021.4.28: 12,327,654
# start_block = 10790000; # 2020.8.28: 10,127,978

# ======================= Raw Attributes =======================
external_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'confirmations']
internal_all = ['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'value', 'contractAddress', 'input', 'type', 'gas', 'gasUsed', 'traceId', 'isError', 'errCode']
erc20_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
erc721_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
# ======================= Selected Attributes =======================
# external_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed'] # tokenName, tokenDecimal
# erc20_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']
# erc721_select = ['blockNumber', 'timeStamp', 'from', 'to', 'tokenID', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']
# ======================= Drop Attributes =======================
external_drop = ['hash', 'nonce', 'blockHash', 'transactionIndex', 'txreceipt_status', 'input', 'confirmations']


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
        df = df.drop(columns=external_drop)
        if not os.path.exists(path):
            # ======================== When Collect Transaction from Contract =========================
            df.to_csv(path)
        else:
            # ======================== When Collect Transaction from Address =========================
            df.to_csv(path, mode='a', header=False)
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
        if not os.path.exists(path):
            # ======================== When Collect Transaction from Contract =========================
            df.to_csv(path)
        else:
            # ======================== When Collect Transaction from Address =========================
            df.to_csv(path, mode='a', header=False)
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
        if not os.path.exists(path):
            # ======================== When Collect Transaction from Contract =========================
            df.to_csv(path)
        else:
            # ======================== When Collect Transaction from Address =========================
            df.to_csv(path, mode='a', header=False)
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

def collect_contract(address:str, start_block:int, end_block:int, save_path:str):
    index = 0
    block_index = start_block
    error_list = []
    while block_index < end_block:
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
    
def collect_address(address_list_path:str, start_block:int, end_block:int, save_path:str):
    address_list = np.load(address_list_path, allow_pickle = True)[71:]
    address_len = len(address_list)
    address_index = 0
    
    for address in address_list[:]:
        error_list = []
        index = 0
        block_index = start_block

        while block_index < end_block:
            status, result = get_external_transaction(address, block_index, block_index + 100000, save_path + address + '.csv')
            # status, result = get_ERC20_transaction(address, block_index, block_index +  10000, save_path)
            if status == '1':
                print(f"{address_index}/{address_len}; {address}; {block_index} to {block_index + 100000}; Status:", result)
            else:
                print(f"{address_index}/{address_len}; {address}; {block_index} to {block_index + 100000}; Status:", result)
                error_list.append((address, block_index, result))
            index += 1
            block_index += 100000

        print(f"Processed address: {address_index}/{address_len}.")
        address_index += 1

        # ============================= DEV: Loop Control ===================================
        # break

    if error_list != []:
        with open(f'./data/error_log/{datetime.datetime.now()}.txt', 'w') as f:
            f.write(str(error_list))
    
    return

def collect_address_lite(address_list_path:str, save_path:str):
    address_list = np.load(address_list_path, allow_pickle = True)[43671:] #479+424+1728+14600+899+7230+15958
    address_total = len(address_list)
    address_index = 0   
    error_list = []


    for address in address_list:
        try:
            status, result = get_external_transaction(address, 10000000, 12327654, save_path + address + '.csv')
            # status, result = get_ERC20_transaction(address, block_index, block_index +  10000, save_path)
            if status == '1':
                print(f"{address_index}/{address_total}; {address}; Status:", result)
            else:
                print(f"{address_index}/{address_total}; {address}; Status:", result)
                error_list.append((address, result))
        except:
            print(f"{address_index}/{address_total}; {address}; Status:", result)
            error_list.append((address, 'Program Error'))
        address_index += 1

        # ============================= DEV: Loop Control ===================================
        # break
        # if address_index % 500 == 0 and error_list != []:
        #     with open(f'./data/error_log/{datetime.datetime.now()}.txt', 'w') as f:
        #         f.write(str(error_list))
        #         error_list = []
        # else:
        #     pass

    if error_list != []:
        with open(f'error_list.txt', 'w') as f:
            f.write(str(error_list))
    
    return


if __name__ == '__main__':
    # ======================= Start Program: Scrape Contract =======================
    # collect(uniswap_v2_router_address, start_block, end_block, './source/data/uniswap_v2/erc721/')
    # collect_contract(sushiswap_router_address, start_block, end_block, './source/data/sushiswap/erc20/')

    # ======================= Start Program: Scrape User Address =======================
    # 212 * 50000 = 10600000 requests
    # Use a count down mode to scrape address info.
    collect_address('./data/overlap_larger_10000.npy', 10200000, 12327654,'./data/overlap_address/external_large/')
