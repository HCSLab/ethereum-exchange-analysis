import requests, os
import numpy as np
import pandas as pd

api_key = '39M8BBF53U6M7N2YS92M163RP3RCZF6GUK'
target = '0x00004242f4449d49ec9c64ad6f9385a5'
api_link = f'https://api.etherscan.io/api?module=account&action=txlist&address={target}&startblock=10200000&endblock=12327654&page=1&offset=1000&sort=asc&apikey={api_key}'
path = './test-1.csv'
external_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'confirmations']

r = eval(requests.get(api_link).text)
if r['status'] == '1':
    value_array = np.array([list(row.values()) for row in r['result']])
    df = pd.DataFrame(value_array, columns=external_all)
    df['type'] = 'external'
    df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
    df = df.set_index('timeStamp')
    df.fillna('NA')
    if not os.path.exists(path):
        # ======================== When Collect Transaction from Contract =========================
        df.to_csv(path)
    else:
        # ======================== When Collect Transaction from Address =========================
        df.to_csv(path, mode='a', header=False)
    print(r['status'], 'OK')
else:
    print(r['status'], r['message'])