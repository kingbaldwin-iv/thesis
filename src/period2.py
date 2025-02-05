from dataclasses import dataclass
import json
from web3 import Web3
import polars as pl
from typing import List, Dict, Set, Union
import logging
from utils import Provider
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DEBUG = False


@dataclass
class Token:
    ca: str
    symbol: str
    decimals: str

    def __str__(self) -> str:
        return json.dumps(self.__dict__, indent=2)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other) -> bool:
        if not isinstance(other, Token):
            return False
        return self.ca == other.ca

    def __hash__(self) -> int:
        return hash(self.ca)


@dataclass
class Pool:
    ca: str
    token0: Token
    token1: Token
    is_v3: bool

    def __str__(self) -> str:
        return json.dumps(
            {
                "Pool Contract": self.ca,
                "is_v3": self.is_v3,
                "Token0": json.loads(self.token0.__str__()),
                "Token1": json.loads(self.token1.__str__()),
            },
            indent=2,
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other) -> bool:
        if not isinstance(other, Pool):
            return False
        return self.ca == other.ca

    def __hash__(self) -> int:
        return hash(self.ca)


@dataclass
class Swap_x:
    block_number: int
    transaction_index: int
    log_index: int
    transaction_hash: str
    pool: Pool
    topic0: str
    sender: str
    recipient: str
    chain_id: int
    pool_delta_t0_unnormalized: float
    pool_delta_t1_unnormalized: float
    user_delta_t0_normalized: float
    user_delta_t1_normalized: float
    token0_buy: bool
    execution_price: float

    def __str__(self) -> str:
        return json.dumps(
            {
                "Log Index": self.log_index,
                "Topic0": self.topic0,
                "Pool": json.loads(self.pool.__str__()),
                "Delta0": self.pool_delta_t0_unnormalized,
                "Delta1": self.pool_delta_t1_unnormalized,
                "User Delta T0": self.user_delta_t0_normalized,
                "User Delta T1": self.user_delta_t1_normalized,
                "Token0 Out": self.token0_buy,
                "Event Sender": self.sender,
                "Execution Price": self.execution_price,
                "Event Recipient": self.recipient,
                "Tx hash": self.transaction_hash,
            },
            indent=2,
        )

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class SwapV3(Swap_x):
    sqrt_price_x86: float
    liquidity: float
    tick: int
    price_after_swap: float

    @staticmethod
    def process_log_v3(
        swaps_df: pl.DataFrame,
        tokens: str,
        pools: str,
    ) -> List[Swap_x]:
        logging.info("Started processing v3 logs!!!")
        ret_list = []

        pools_df = pl.read_parquet(pools)
        tokens_df = pl.read_parquet(tokens)

        chain_id = swaps_df["chain_id"][0]
        rows = 100000 if DEBUG else swaps_df.shape[0]

        for row in range(0, rows):  # range(0,rows)
            tx_index = swaps_df["transaction_index"][row]
            block_no = swaps_df["block_number"][row]
            tx_hash = "0x" + swaps_df["transaction_hash"][row].hex()
            pool_ca = Web3.to_checksum_address("0x" + swaps_df["address"][row].hex())
            try:
                pool_row = pools_df.row(
                    by_predicate=(pl.col("pool_address") == pool_ca)
                )
                token0_ca = pool_row[1]
                token1_ca = pool_row[2]
                token0_row = tokens_df.row(
                    by_predicate=(pl.col("contract_address") == token0_ca)
                )
                token1_row = tokens_df.row(
                    by_predicate=(pl.col("contract_address") == token1_ca)
                )
            except Exception:
                logging.info(f"Exception occured while processing swap v3 row {row}")
                continue
            token0_sym = token0_row[0]
            token1_sym = token1_row[0]
            token0_decimal = token0_row[1]
            token1_decimal = token1_row[1]
            token0 = Token(ca=token0_ca, symbol=token0_sym, decimals=token0_decimal)
            token1 = Token(ca=token1_ca, symbol=token1_sym, decimals=token1_decimal)
            my_pool = Pool(ca=pool_ca, token0=token0, token1=token1, is_v3=True)
            log_index = swaps_df["log_index"][row]
            topic0 = "0x" + swaps_df["topic0"][row].hex()
            pool_delta_t0_unnormalized = swaps_df["event__amount0_f64"][row]
            pool_delta_t1_unnormalized = swaps_df["event__amount1_f64"][row]
            user_delta_t0_normalized = -pool_delta_t0_unnormalized / (
                10**token0.decimals
            )
            user_delta_t1_normalized = -pool_delta_t1_unnormalized / (
                10**token1.decimals
            )
            execution_price = (
                (-user_delta_t1_normalized / user_delta_t0_normalized)
                if user_delta_t0_normalized != 0.0
                else 0.0
            )
            token0_out = True if user_delta_t0_normalized > 0 else False
            sqrt_price_x86 = swaps_df["event__sqrtPriceX96_f64"][row]
            price_after_swap = (sqrt_price_x86**2) / (2**192) * 10**12
            sender = Web3.to_checksum_address(
                "0x" + swaps_df["event__sender"][row].hex()
            )
            recipient = Web3.to_checksum_address(
                "0x" + swaps_df["event__recipient"][row].hex()
            )
            sw = SwapV3(
                block_number=block_no,
                transaction_index=tx_index,
                log_index=log_index,
                transaction_hash=tx_hash,
                pool=my_pool,
                topic0=topic0,
                sender=sender,
                recipient=recipient,
                chain_id=chain_id,
                pool_delta_t0_unnormalized=pool_delta_t0_unnormalized,
                pool_delta_t1_unnormalized=pool_delta_t1_unnormalized,
                price_after_swap=price_after_swap,
                user_delta_t0_normalized=user_delta_t0_normalized,
                user_delta_t1_normalized=user_delta_t1_normalized,
                token0_buy=token0_out,
                execution_price=execution_price,
                sqrt_price_x86=sqrt_price_x86,
                liquidity=swaps_df["event__liquidity_f64"][row],
                tick=swaps_df["event__tick"][row],
            )
            ret_list.append(sw)
            print(f"Done with {(100 * row / rows):.4f}%", end="\r")

        logging.info("Finished v3 logs!!!")
        return ret_list


@dataclass
class SwapV2(Swap_x):
    @staticmethod
    def process_log_v2(
        swaps_df: pl.DataFrame,
        tokens: str,
        pools: str,
    ) -> List[Swap_x]:
        ret_list = []
        logging.info("Started processing v2 logs!!!")
        pools_df = pl.read_parquet(pools)
        tokens_df = pl.read_parquet(tokens)

        chain_id = swaps_df["chain_id"][0]
        rows = 100000 if DEBUG else swaps_df.shape[0]

        for row in range(0, rows):  # range(0,rows)
            tx_index = swaps_df["transaction_index"][row]
            block_no = swaps_df["block_number"][row]
            tx_hash = "0x" + swaps_df["transaction_hash"][row].hex()
            pool_ca = Web3.to_checksum_address("0x" + swaps_df["address"][row].hex())
            try:
                pool_row = pools_df.row(
                    by_predicate=(pl.col("pool_address") == pool_ca)
                )
                token0_ca = pool_row[1]
                token1_ca = pool_row[2]
                token0_row = tokens_df.row(
                    by_predicate=(pl.col("contract_address") == token0_ca)
                )
                token1_row = tokens_df.row(
                    by_predicate=(pl.col("contract_address") == token1_ca)
                )
            except Exception:
                logging.info(f"Exception occured during v2 swap log row {row}")
                continue
            token0_sym = token0_row[0]
            token1_sym = token1_row[0]
            token0_decimal = token0_row[1]
            token1_decimal = token1_row[1]
            token0 = Token(ca=token0_ca, symbol=token0_sym, decimals=token0_decimal)
            token1 = Token(ca=token1_ca, symbol=token1_sym, decimals=token1_decimal)
            my_pool = Pool(ca=pool_ca, token0=token0, token1=token1, is_v3=False)
            log_index = swaps_df["log_index"][row]
            topic0 = "0x" + swaps_df["topic0"][row].hex()
            pool_delta_t0_unnormalized = (
                swaps_df["event__amount0In_f64"][row]
                - swaps_df["event__amount0Out_f64"][row]
            )
            pool_delta_t1_unnormalized = (
                swaps_df["event__amount1In_f64"][row]
                - swaps_df["event__amount1Out_f64"][row]
            )
            user_delta_t0_normalized = -pool_delta_t0_unnormalized / (
                10**token0.decimals
            )
            user_delta_t1_normalized = -pool_delta_t1_unnormalized / (
                10**token1.decimals
            )
            execution_price = (
                (-user_delta_t1_normalized / user_delta_t0_normalized)
                if user_delta_t0_normalized != 0.0
                else 0.0
            )
            token0_out = True if user_delta_t0_normalized > 0 else False
            sender = Web3.to_checksum_address(
                "0x" + swaps_df["event__sender"][row].hex()
            )
            recipient = Web3.to_checksum_address(
                "0x" + swaps_df["event__to"][row].hex()
            )
            sw = SwapV2(
                block_number=block_no,
                transaction_index=tx_index,
                log_index=log_index,
                transaction_hash=tx_hash,
                pool=my_pool,
                topic0=topic0,
                sender=sender,
                recipient=recipient,
                chain_id=chain_id,
                pool_delta_t0_unnormalized=pool_delta_t0_unnormalized,
                pool_delta_t1_unnormalized=pool_delta_t1_unnormalized,
                user_delta_t0_normalized=user_delta_t0_normalized,
                user_delta_t1_normalized=user_delta_t1_normalized,
                token0_buy=token0_out,
                execution_price=execution_price,
            )
            ret_list.append(sw)
            print(f"Done with {(100 * row / rows):.4f}%", end="\r")

        logging.info("Finished processing v2 logs!!!")
        return ret_list


@dataclass
class Transaction:
    block_number: int
    transaction_index: int
    transaction_hash: str
    swaps: List[Swap_x]

    def analyze(self) -> (bool, Dict[str, Union[int, float, str, List[str]]]):
        pools: Set[Pool] = set(map(lambda x: x.pool, self.swaps))
        tokens: Set[Token] = set()
        for pool in pools:
            tokens.add(pool.token1)
            tokens.add(pool.token0)
        self.swaps.sort(key=lambda x: x.log_index)
        balance_changes: Dict[str, float] = {}
        for swap in self.swaps:
            token0_sym = swap.pool.token0.symbol
            token1_sym = swap.pool.token1.symbol
            if token0_sym in balance_changes.keys():
                balance_changes[token0_sym] = (
                    balance_changes[token0_sym] + swap.user_delta_t0_normalized
                )
            else:
                balance_changes[token0_sym] = swap.user_delta_t0_normalized
            if token1_sym in balance_changes.keys():
                balance_changes[token1_sym] = (
                    balance_changes[token1_sym] + swap.user_delta_t1_normalized
                )
            else:
                balance_changes[token1_sym] = swap.user_delta_t1_normalized
        self.balance_changes = balance_changes

        ins: Set[Token] = set()
        outs: Set[Token] = set()
        factor4 = True
        stack4: List[Token] = []
        for swap in self.swaps:
            tin = swap.pool.token1 if swap.token0_buy else swap.pool.token0
            tout = swap.pool.token0 if swap.token0_buy else swap.pool.token1
            if factor4:
                if len(stack4) == 0:
                    stack4.append(tin)
                    stack4.append(tout)
                else:
                    val = stack4.pop()
                    if val != tin:
                        factor4 = False
                    stack4.append(tout)
            outs.add(tout)
            if tin not in outs:
                ins.add(tin)
        sec = ins.intersection(outs)
        t_in = (
            self.swaps[0].pool.token1.symbol
            if self.swaps[0].token0_buy
            else self.swaps[0].pool.token0.symbol
        )
        last = len(self.swaps) - 1
        t_out = (
            self.swaps[last].pool.token0.symbol
            if self.swaps[last].token0_buy
            else self.swaps[last].pool.token1.symbol
        )
        deltas = list(balance_changes.values())
        factor1 = len(sec) > 0
        factor2 = t_out == t_in
        factor3 = len(deltas) - 1 == deltas.count(0.0)
        factor4 = factor4 and len(stack4) == 2 and stack4[0] == stack4[1]
        path_str = t_in
        if factor1 and factor2 and factor3 and factor4:
            senders = set()
            recipients = set()
            for swap in self.swaps:
                senders.add(swap.sender)
                recipients.add(swap.recipient)
                if swap.token0_buy:
                    path_str = path_str + f"->{swap.pool.token0.symbol}"
                else:
                    path_str = path_str + f"->{swap.pool.token1.symbol}"
            self.profit_token_amount = [
                (x, balance_changes[x])
                for x in balance_changes
                if balance_changes[x] != 0.0
            ][0]
            ret_dict = {
                "transaction_hash": self.transaction_hash,
                "block_number": self.block_number,
                "transaction_index": self.transaction_index,
                "profit_token": self.profit_token_amount[0],
                "profit_amount": self.profit_token_amount[1],
                "path": path_str,
                "senders": list(senders),
            }
            return (True, ret_dict)
        return (False, {})

    @staticmethod
    def bundle_swaps(swaps: List[Swap_x]) -> List["Transaction"]:
        logging.info("Started bundling swaps!!!")
        indexer = {}
        txs = []
        new_idx = 0
        for s in swaps:
            if s.transaction_hash in indexer.keys():
                txs[indexer[s.transaction_hash]].append(s)
            else:
                indexer[s.transaction_hash] = new_idx
                txs.append([s])
                new_idx = new_idx + 1
        txs_ret = []
        for tx in txs:
            txs_ret.append(
                Transaction(
                    block_number=tx[0].block_number,
                    transaction_index=tx[0].transaction_index,
                    transaction_hash=tx[0].transaction_hash,
                    swaps=tx,
                )
            )

        logging.info("Done with bundling swaps!!!")
        return txs_ret

    def __str__(self) -> str:
        prox = list(map(lambda x: json.loads(x.__str__()), self.swaps))
        return json.dumps(
            {
                "Block no": self.block_number,
                "Tx index": self.transaction_index,
                "Tx hash": self.transaction_hash,
                "Swaps": prox,
            },
            indent=5,
        )

    def __repr__(self) -> str:
        return self.__str__()


def fetch_swap_data(path: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    path_v2 = path + "__logs__v2*.parquet"
    path_v3 = path + "__logs__v3*.parquet"
    df2 = pl.read_parquet(path_v2)
    df3 = pl.read_parquet(path_v3)
    return (df2, df3)


def process_chain(path: str, provider: Provider):
    (df2, df3) = fetch_swap_data(path)
    chain_name = path.split("/")[1]
    path_tokens = f"{chain_name}_tokens.parquet"
    path_pools = f"{chain_name}_pools.parquet"
    range_low = (
        provider.start_block_arbitrum
        if chain_name == "arbitrum"
        else provider.start_block_optimism
    )
    range_high = (
        provider.end_block_arbitrum
        if chain_name == "arbitrum"
        else provider.end_block_optimism
    )
    curr = range_low
    delta = (range_high - range_low) // 50
    while curr <= range_high:
        curr_high = curr + delta
        df2x = df2.filter(
            (pl.col("block_number") >= curr) & (pl.col("block_number") < curr_high)
        )
        df3x = df3.filter(
            (pl.col("block_number") >= curr) & (pl.col("block_number") < curr_high)
        )
        file_name = f"{chain_name}_{curr}_{curr_high}_mev.parquet"
        if os.path.exists(file_name):
            logging.info(f"Already done: {file_name}, skipping!!")
            curr = curr_high
            continue
        logging.info(f"Operating on {file_name}!!!")

        l3 = SwapV3.process_log_v3(df3x, path_tokens, path_pools)
        l2 = SwapV2.process_log_v2(df2x, path_tokens, path_pools)
        l3.extend(l2)
        txs = Transaction.bundle_swaps(l3)
        mev = []
        logging.info("Processing txs!!!")
        total = len(txs)
        done = 0
        for x in txs:
            a = x.analyze()
            if a[0]:
                mev.append(a[1])
            done = done + 1
            print(f"Done with {(100 * done / total):.4f}%", end="\r")
        mev_df = pl.DataFrame(mev)
        mev_df.write_parquet(f"{chain_name}_{curr}_{curr_high}_mev.parquet")
        curr = curr_high


def main():
    logging.info("Started Processing!!!")
    provider = Provider.generate()
    paths = ["./arbitrum/arbitrum", "./optimism/optimism"]  # "./base/base"]
    for i in paths:
        logging.info(f"Processing {i}!!!")
        process_chain(i, provider)
        logging.info(f"Processing {i} done!!!")


if __name__ == "__main__":
    main()
