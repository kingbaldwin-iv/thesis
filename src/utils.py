import json
import os
from dataclasses import dataclass


@dataclass
class Provider:
    arbitrum_rpc: str
    base_rpc: str
    optimism_rpc: str
    start_block_arbitrum: int
    end_block_arbitrum: int
    start_block_base: int
    end_block_base: int
    start_block_optimism: int
    end_block_optimism: int

    @staticmethod
    def generate() -> "Provider":
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        with open(config_path) as f:
            config = json.load(f)
        return Provider(
            arbitrum_rpc=config["arbitrum_rpc"],
            base_rpc=config["base_rpc"],
            optimism_rpc=config["optimism_rpc"],
            start_block_arbitrum=config["start_block_arbitrum"],
            end_block_arbitrum=config["end_block_arbitrum"],
            start_block_base=config["start_block_base"],
            end_block_base=config["end_block_base"],
            start_block_optimism=config["start_block_optimism"],
            end_block_optimism=config["end_block_optimism"],
        )
