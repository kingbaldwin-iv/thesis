import polars as pl
import requests
import json
from utils import Provider
from web3 import Web3
from tqdm import tqdm
import multiprocessing
FUNCTION_MAP = {
    "0xa9059cbb": "erc20_transfer",
    "0x23b872dd": "erc20_transferFrom",
    "0x38ed1739": "Uniswap V2 Swap",
    "0xc04b8d59": "Uniswap V3 Swap",
    "0x128acb08": "univ3_swap",
    "0x022c0d9f": "univ2_swap",
    "0xa9059cbb": "univ2_transfer",
    "0x23b872dd": "univ2_transferFrom",
    "0xb93f9b0a": "uni_getAddress",
    "0x70a08231": "uni_balanceOf",
    "0x0dfe1681": "uni_token0",
    "0x5c60da1b": "uni_implementation",
    "0x313ce567": "uni_decimals",
    "0x654b6487": "ramsesV2SwapCallback",
    "0xd21220a7": "uni_token1",
    "0xdd62ed3e": "allowance",
    "0xfa461e33": "uniswapV3SwapCallback",
}
adig = set(map(lambda x: x.lower(), pl.read_parquet("../arbitrum_pools.parquet")["pool_address"].to_list())).union(set(map(lambda x: x.lower(), pl.read_parquet("../arbitrum_tokens.parquet")["contract_address"].to_list())))
ARBITRUM_DEBUG_RPC = Provider.generate().arbitrum_rpc

def flatten_data(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out
def main2(tx_hash):
    TX_HASH = tx_hash
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [TX_HASH, {"tracer": "callTracer"}],
        "id": 1
    }
    response = requests.post(ARBITRUM_DEBUG_RPC, json=payload).json()
    d = flatten_data(response)
    address_to_associated_sig = dict() 
    for x in d.keys():
        if "input" in x:
            sel = d[x][:10]
            a1 = d[x[:-5]+"from"]
            a2 = d[x[:-5]+"to"]
            v1 = address_to_associated_sig.get(a1,set())
            v1.add(sel)
            v2 = address_to_associated_sig.get(a2,set())
            v2.add(sel)
            address_to_associated_sig[a1] = v1
            address_to_associated_sig[a2] = v2
    additional_addresses_to_ignore = set()
    for x in address_to_associated_sig.keys():
        if len(address_to_associated_sig[x] - set(FUNCTION_MAP.keys())) == 0:
            additional_addresses_to_ignore.add(x.lower())
    additional_addresses_to_ignore = adig.union(additional_addresses_to_ignore)
    filtered_out = set()
    for x in d.keys():
        if x[-3:] == "_to":
            ff = x[:-3] + "_from"
            if d[ff] == None or d[x] == None: continue
            to_check = set([d[ff].lower(),d[x].lower()])
            if len(to_check.intersection(additional_addresses_to_ignore)) == 0:
                filtered_out.add(x[:-3])

    df = list()
    for x in filtered_out:
        v = {"tx_hash":TX_HASH,"flattened_trace": x, "to": d[x+"_to"], "from": d[x+"_from"], "function_selector":d[x+"_input"][:10], "gas_used":int(d[x+"_gasUsed"],16)}
        df.append(v)
    return df

if __name__ == "__main__":
    num_cores = multiprocessing.cpu_count()  # Get number of CPU cores
    print(f"Using {num_cores} cores")
    df = pl.read_parquet("arbitrum_mev.parquet")["transaction_hash"].to_list()
    
    with multiprocessing.Pool(processes=num_cores) as pool:
        results = list(tqdm(pool.imap(main2, df), total=len(df)))
    ret = list()
    for x in results:
        for y in x:
            ret.append(y)
    
    dd = pl.DataFrame(ret)
    print(dd)
    dd.write_parquet("interesting_traces_arb.parquet")
