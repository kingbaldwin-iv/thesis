#!/usr/bin/env bash

BLOCKS_OPTIMISM="126100000:130050000"
BLOCKS_ARBITRUM="259100000:290700000"
BLOCKS_BASE="20500000:24480000"

cryo logs \
    --blocks $BLOCKS_OPTIMISM \
    --event-signature "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)" \
    --topic0 "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" \
    --n-chunks 100 \
    --output-dir optimism \
    --label v3_optimism \
    --max-concurrent-requests 16 \
    --rpc disco_op \
    --no-report

cryo logs \
    --blocks $BLOCKS_OPTIMISM \
    --event-signature "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)" \
    --topic0 "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" \
    --n-chunks 100 \
    --output-dir optimism \
    --label v2_optimism \
    --max-concurrent-requests 16 \
    --rpc disco_op \
    --no-report

cryo logs \
    --blocks $BLOCKS_ARBITRUM \
    --event-signature "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)" \
    --topic0 "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" \
    --n-chunks 100 \
    --output-dir arbitrum \
    --label v3_arbitrum \
    --max-concurrent-requests 16 \
    --rpc disco_arb \
    --no-report

cryo logs \
    --blocks $BLOCKS_ARBITRUM \
    --event-signature "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)" \
    --topic0 "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" \
    --n-chunks 100 \
    --output-dir arbitrum \
    --label v2_arbitrum \
    --max-concurrent-requests 16 \
    --rpc disco_arb \
    --no-report

cryo logs \
    --blocks $BLOCKS_BASE \
    --event-signature "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)" \
    --topic0 "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" \
    --n-chunks 100\
    --output-dir base \
    --label v3_base \
    --max-concurrent-requests 16 \
    --rpc disco_base \
    --no-report

cryo logs \
    --blocks $BLOCKS_BASE \
    --event-signature "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)" \
    --topic0 "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" \
    --n-chunks 100\
    --output-dir base \
    --label v2_base \
    --max-concurrent-requests 16 \
    --rpc disco_base \
    --no-report



