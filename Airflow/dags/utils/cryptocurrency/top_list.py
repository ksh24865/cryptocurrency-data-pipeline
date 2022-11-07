import logging
from typing import Iterable, List

from airflow.providers.http.hooks.http import HttpHook
from constants import CRYPTO_COMPARE_HTTP_CONN_ID
from utils.list import list_chunk_generator
from utils.request import get_request_json

# temporary list, actually stored in redis
TOP_SYMBOL_LIST_BY_MARKET_CAP = [
    "BTC",
    "ETH",
    "XRP",
    "USDT",
    "LUNA",
    "BNB",
    "SOL",
    "AVAX",
    "ADA",
    "DOT",
    "BUSD",
    "DOGE",
    "UST",
    "MATIC",
    "LINK",
    "FTT",
    "SHIB",
    "AXS",
    "CRO",
    "KLAY",
    "WBTC",
    "BIT",
    "XLM",
    "UNI",
    "DAI",
    "SAND",
    "GALA",
    "ICP",
    "ATOM",
    "LTC",
    "NEAR",
    "TRX",
    "BCH",
    "LEO",
    "MANA",
    "OKB",
    "ALGO",
    "DYDX",
    "USDC",
    "RUNE",
    "ETC",
    "VET",
    "QNT",
    "GRT",
    "HBAR",
    "IMX",
    "ILV",
    "EGLD",
    "CRV",
    "XMR",
    "LDO",
    "WAVES",
    "FTM",
    "FIL",
    "GNO",
    "KCS",
    "PCI",
    "THETA",
    "XTZ",
    "RLY",
    "DFI",
    "CELO",
    "ANC",
    "YGG",
    "HNT",
    "AMP",
    "AAVE",
    "ROSE",
    "1INCH",
    "EOS",
    "NEO",
    "CEL",
    "NEXO",
    "MC",
    "ZEC",
    "CUSDC",
    "AR",
    "FLOW",
    "MIOTA",
    "MKR",
    "XDC",
    "HT",
    "MINA",
    "JASMY",
    "ONE",
    "CHZ",
    "FXS",
    "CAKE",
    "STX",
    "NU",
    "CVX",
    "XCH",
    "ENS",
    "BSV",
    "RAY",
    "XEC",
    "XRD",
    "ENJ",
    "RNDR",
    "LYXE",
]


def get_top_symbol_list_by_market_cap(
    tsym: str = "USD",
    limit: int = 100,
    http_hook: HttpHook = HttpHook(
        http_conn_id=CRYPTO_COMPARE_HTTP_CONN_ID,
        method="GET",
    ),
) -> List[str]:
    top_info_list_by_market_cap = get_request_json(
        http_hook=http_hook,
        endpoint="top/mktcapfull",
        data={
            "tsym": tsym,
            "limit": limit,
        },
    )

    top_symbol_list_by_market_cap = [
        info.get("CoinInfo", {}).get("Name") for info in top_info_list_by_market_cap
    ]

    logging.info(f"top_symbol_list_by_market_cap: {top_symbol_list_by_market_cap}")

    return top_symbol_list_by_market_cap


def top_symbol_list_by_market_cap_generator(
    chunk_size: int = 10,
) -> Iterable[List[str]]:
    return list_chunk_generator(
        arr=TOP_SYMBOL_LIST_BY_MARKET_CAP,
        chunk_size=chunk_size,
    )
