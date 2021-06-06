from numpy.lib.npyio import save
import pandas as pd
import numpy as np
import os
#========================================================
uniswap_v2_router_address = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
sushiswap_router_address = '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f'
#========================================================
external_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'confirmations']
internal_all = ['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'value', 'contractAddress', 'input', 'type', 'gas', 'gasUsed', 'traceId', 'isError', 'errCode']
erc20_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
erc721_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']    
#========================================================
external_select = ['timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed']
internal_select = ['timeStamp', 'from', 'to', 'value', 'contractAddress', 'gas', 'gasUsed', 'traceId', 'isError']
erc20_select = ['timeStamp', 'from', 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'confirmations']
erc721_select = ['timeStamp', 'from', 'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'confirmations']    
#========================================================
# if collect address in external target = 'from'
# if collect address in internal target = 'to'
#========================================================
def collect_interact_address(input_path:str, output_path:str ,address:str, target:str):
    file_names = os.listdir(input_path)
    for name in file_names:
        df = pd.read_csv(input_path + name)
        break
    return

def concate_csv(input_path:str, output_path:str, select_attr:list):
    file_names = os.listdir(input_path)
    total = len(file_names)
    df = pd.read_csv(input_path + file_names[0])[select_attr]
    df.to_csv(output_path)
    index = 1
    for name in file_names[1:]:
        df = pd.read_csv(input_path + name)[select_attr]
        df.to_csv(output_path, mode='a', header=False)
        print(f"{index}/{total}")
        index += 1
    return

def describe_dataframe(input_path:str, contract_address:str):
    df = pd.read_csv(input_path)
    print("length:", len(df))
    print("===========================")
    print("unique:", len(np.unique(np.append(df['from'].values, df['to'].values))))
    print("===========================")
    df['value'] = df['value'].apply(lambda x: int(x) / 1000000000000000000)
    print("value mean", np.mean(df['value'].values))
    print("value median", np.median(df['value'].values))
    print("value std", np.std(df['value'].values))
    print("===========================")
    print(df[['gas']].describe())
    return

def find_overlap(input_path_1:str, input_path_2:str, save_path:str, erc20_flag, attribute='from'):
    if erc20_flag:
        address_1 = np.unique(np.append(pd.read_csv(input_path_1)['from'].values, pd.read_csv(input_path_1)['to'].values))
        address_2 = np.unique(np.append(pd.read_csv(input_path_2)['from'].values, pd.read_csv(input_path_2)['to'].values))
        print("Unique address in input 1 is: ", len(address_1))
        print("Unique address in input 2 is: ", len(address_2))
        print("========================================================")
        overlap = np.intersect1d(address_1, address_2)
        print("Overlap address length: ", len(overlap))
        np.save(save_path, overlap)
    else:
        address_1 = np.unique(pd.read_csv(input_path_1)[attribute].values)
        address_2 = np.unique(pd.read_csv(input_path_2)[attribute].values)
        print("Unique address in input 1 is: ", len(address_1))
        print("Unique address in input 2 is: ", len(address_2))
        print("========================================================")
        overlap = np.intersect1d(address_1, address_2)
        print("Overlap address length: ", len(overlap))
        np.save(save_path, overlap)
    return

def summarize_overlap(input_path_1:str, input_path_2:str, input_path_3:str, save_path:str):
    temp = np.unique(np.append(np.load(input_path_1, allow_pickle=True), np.load(input_path_2, allow_pickle=True)))
    temp = np.unique(np.append(np.load(input_path_3, allow_pickle=True), temp))
    np.save(save_path, temp)
    print("Done, saved to: ", save_path)
    return

if __name__ == '__main__':
    print(len(np.load('./data/overlap_all_univ2-sushi.npy', allow_pickle=True)))
    
    appName = 'uniswap_v2'
    transactionType = 'erc20'
    #========================================================
    # collect_interact_address('./data/sushiswap/external/', './data/sushiswap/', sushiswap_router_address, 'from')
    #========================================================
    # concate_csv(f'./data/{appName}/{transactionType}/', f'./data/{appName}/{transactionType}.csv', erc20_select)
    #========================================================
    # describe_dataframe(f'./data/{appName}/{transactionType}.csv', uniswap_v2_router_address)
    #========================================================
    # find_overlap('./data/uniswap_v2/erc20.csv', './data/sushiswap/erc20.csv', './data/overlap_erc20_univ2-sushi.npy', True, 'to')
    #========================================================
    # summarize_overlap('./data/overlap_erc20_univ2-sushi.npy', './data/overlap_external_univ2-sushi.npy', './data/overlap_internal_univ2-sushi.npy', './data/overlap_all_univ2-sushi.npy')