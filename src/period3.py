import polars as pl
from typing import Dict
from web3 import Web3, HTTPProvider
import logging
import os
import re
from utils import Provider

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_chain(path: str, rpc: str):
    files = [f for f in os.listdir(".") if os.path.isfile(f)]
    pattern = rf"{path}.*mev\.parquet$"
    matches = [s for s in files if re.fullmatch(pattern, s)]
    if len(matches) == 0:
        logging.warning(f"Mev files for {path} don't exist!!!")
        return
    resulting_fn = f"{path}mev_address_summary.parquet"
    if os.path.exists(resulting_fn):
        return
    mev_df = pl.read_parquet(f"{path}*mev.parquet")
    print(mev_df)
    w3 = Web3(HTTPProvider(rpc))
    rows = mev_df.shape[0]
    ref: Dict[str, int] = {}
    logging.info("Processing mev data!!!")
    for i in range(0, rows):
        addresses = set(mev_df["senders"][i].to_list())
        for x in addresses:
            if x in ref.keys():
                ref[x] = ref[x] + 1
            else:
                ref[x] = 1
        print(f"Done with {(100 * i / rows):.4f}%", end="\r")
    ref = {k: v for k, v in sorted(ref.items(), key=lambda item: -item[1])}
    address_summary_df = []
    logging.info("Queying contracts!!!")
    to_do = len(ref)
    done = 0
    for i in ref.keys():
        count = ref[i]
        code = w3.eth.get_code(i)
        profit_tokens = (
            mev_df.filter(pl.col("senders").list.contains(i))
            .group_by("profit_token")
            .count()
            .sort(-1 * pl.col("count"))
        )
        main_profit_token = profit_tokens["profit_token"][0]
        main_p_count = profit_tokens["count"][0]
        lentt = len("0x" + code.hex())
        val = {
            "address": i,
            "count": count,
            "main_profit_token": main_profit_token,
            "count_main": main_p_count,
            "bytecode": code,
            "bytecode_length": lentt,
        }
        address_summary_df.append(val)
        done = done + 1
        print(f"Done with {(100 * done / to_do):.4f}%", end="\r")
    address_summary_dfed = pl.DataFrame(address_summary_df)
    address_summary_dfed = address_summary_dfed.sort(pl.col("bytecode_length") * -1)
    address_summary_dfed.write_parquet(resulting_fn)


def main():
    logging.info("Started Processing!!!")
    provider = Provider.generate()
    paths_rpc = [
        ("optimism_", provider.optimism_rpc),
        ("arbitrum_", provider.arbitrum_rpc),
        ("base_", provider.base_rpc),
    ]
    for i in paths_rpc:
        logging.info(f"Processing {i}!!!")
        process_chain(i[0], i[1])
        logging.info(f"Processing {i} done!!!")


if __name__ == "__main__":
    main()
