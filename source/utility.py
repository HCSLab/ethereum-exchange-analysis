from numpy.lib.function_base import append
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
external_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'type']
#========================================================
external_drop = ['hash', 'nonce', 'blockHash', 'transactionIndex', 'txreceipt_status', 'input', 'confirmations']
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

def add_label(input_path:str, translation_database_path:str):
    file_names = os.listdir(input_path)
    trans_df = pd.read_csv(translation_database_path)[['address','id','title','category','author','balance','contractsCount','ranking']]
    file_index = 0
    file_total = len(file_names)-1
    for file_name in file_names:
        address = file_name[:-4]
        from_title = []
        from_cate = []
        to_title = []
        to_cate = []
        # ===================Labeling Part===================
        df = pd.read_csv(input_path + file_name)
        from_address = df['from'].values
        to_address = df['to'].values
        
        for match_address in from_address:
            if match_address == address:
                from_title.append('self')
                from_cate.append('self')
            else:
                try:
                    from_title.append(trans_df.loc[trans_df['address'] == match_address, 'title'].iloc[0])
                    from_cate.append(trans_df.loc[trans_df['address'] == match_address, 'category'].iloc[0])
                except:
                    from_title.append('unknown')
                    from_cate.append('unknown')

        df['from_title'] = np.array(from_title)
        df['from_cate'] = np.array(from_cate)
        
        for match_address in to_address:
            if match_address == address:
                to_title.append('self')
                to_cate.append('self')
            else:
                try:
                    to_title.append(trans_df.loc[trans_df['address'] == match_address, 'title'].iloc[0])
                    to_cate.append(trans_df.loc[trans_df['address'] == match_address, 'category'].iloc[0])
                except:
                    to_title.append('unknown')
                    to_cate.append('unknown')

        df['to_title'] = np.array(to_title)
        df['to_cate'] = np.array(to_cate)

        df.to_csv(input_path + file_name)

        # ===================Labeling Part===================
        print(f"labeled: {file_index}/{file_total}")
        file_index += 1
        # Loop Control
        # break
    return

def temp_find_tx_larger_10000(input_path:str, save_path):
    # There are 101 address that have external transaction more than 10000 
    file_names = os.listdir(input_path)
    store_list = []
    total = len(file_names)
    index = 0
    for file_name in file_names:
        df = pd.read_csv(input_path + file_name)
        if len(df) == 10000:
            store_list.append(file_name[:-4])
        print(f"{index}/{total}")
        index += 1
    store_list = np.array(store_list)
    np.save(save_path, store_list)
    return
    

def temp_remove_useless_column(input_path:str):
    file_names = os.listdir(input_path)
    total = len(file_names)
    index = 0
    for file_name in file_names:
        df = pd.read_csv(input_path + file_name)
        df = df.drop(columns = external_drop)
        df.to_csv(input_path + file_name)
        print(f"{index}/{total}")
        index += 1
    return

if __name__ == '__main__':
    # print(len(np.load('./data/overlap_all_univ2-sushi.npy', allow_pickle=True)))
    
    # appName = 'uniswap_v2'
    # transactionType = 'erc20'
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
    #========================================================
    # temp_remove_useless_column('./data/overlap_address/external/')
    #========================================================
    # temp_find_tx_larger_10000('./data/overlap_address/temp/', './data/overlap_larger_10000.npy')
    #========================================================
    add_label('./data/overlap_address/external/', './data/translation_database.csv')
