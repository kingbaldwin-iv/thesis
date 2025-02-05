from web3 import Web3, HTTPProvider
import polars as pl
from typing import List, Set
from utils import Provider
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generate_pool_data(path: str) -> tuple[List[str], List[str]]:
    path_v2 = path + "__logs__v2*.parquet"
    path_v3 = path + "__logs__v3*.parquet"
    df2 = pl.read_parquet(path_v2)
    df3 = pl.read_parquet(path_v3)
    v2_addresses = list(
        map(
            lambda x: Web3.to_checksum_address("0x" + x.hex()),
            set(df2["address"].to_list()),
        )
    )
    v3_addresses = list(
        map(
            lambda x: Web3.to_checksum_address("0x" + x.hex()),
            set(df3["address"].to_list()),
        )
    )
    return (v2_addresses, v3_addresses)


def query_pool(addresses: List[str], is_v3: bool, w3) -> tuple[pl.DataFrame, List[str]]:
    minimal_pool_abi = [
        {
            "inputs": [],
            "name": "token0",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "token1",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]
    pl_pool = []
    total = len(addresses)
    current = 0
    tokens = set()
    for i in addresses:
        try:
            contract = w3.eth.contract(address=i, abi=minimal_pool_abi)
            token0 = contract.functions.token0().call()
            token1 = contract.functions.token1().call()
            val = {
                "pool_address": i,
                "token0": token0,
                "token1": token1,
                "is_v3": is_v3,
            }
            pl_pool.append(val)
            tokens.add(token0)
            tokens.add(token1)
            current = current + 1
            print(f"Done with {(100 * current / total):.4f}%", end="\r")
        except Exception:
            logging.warning(f"Failed to process pool: {i}")
    df = pl.DataFrame(pl_pool)
    tokens = list(tokens)
    return (df, tokens)


def query_tokens(addresses: Set[str], w3) -> pl.DataFrame:
    minimal_abi_erc20 = [
        {
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]
    pl_tokens = []
    total = len(addresses)
    current = 0
    for i in addresses:
        try:
            contract = w3.eth.contract(address=i, abi=minimal_abi_erc20)
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            val = {"symbol": symbol, "decimal": decimals, "contract_address": i}
            pl_tokens.append(val)
            current = current + 1
            print(f"Done with {(100 * current / total):.4f}%", end="\r")
        except Exception:
            logging.info(f"Failed to process token {i}")
    df = pl.DataFrame(pl_tokens)
    return df


def process_chain(path: str, rpc: str):
    chain_name = path.split("/")[1]
    if os.path.exists(f"{chain_name}_tokens.parquet") and os.path.exists(
        f"{chain_name}_pools.parquet"
    ):
        return
    w3_provider = Web3(HTTPProvider(rpc))
    logging.info("Fetching pool addresses!!!")
    (v2_addresses, v3_addresses) = generate_pool_data(path)
    logging.info("Pool add. fetched! Querying V2 pools!!!")
    (pool2_df, tokens2) = query_pool(v2_addresses, False, w3_provider)
    logging.info("Querying V3 pools!!!")
    (pool3_df, tokens3) = query_pool(v3_addresses, True, w3_provider)
    tokens2.extend(tokens3)
    pool2_df.extend(pool3_df)
    pool2_df.write_parquet(f"{chain_name}_pools.parquet")
    logging.info("Written pools file!!!")
    tokens2 = set(tokens2)
    logging.info("Querying tokens!!!")
    df_tokens = query_tokens(tokens2, w3_provider)
    df_tokens.write_parquet(f"{chain_name}_tokens.parquet")
    logging.info("Written tokens file!!!")


def main():
    logging.info("Started Processing!!!")
    provider = Provider.generate()
    paths_rpc = [
        ("./arbitrum/arbitrum", provider.arbitrum_rpc),
        ("./optimism/optimism", provider.optimism_rpc),
    ]  # ("./base/base",provider.base_rpc)]
    for i in paths_rpc:
        logging.info(f"Processing {i}!!!")
        process_chain(i[0], i[1])
        logging.info(f"Processing {i} done!!!")


if __name__ == "__main__":
    main()
