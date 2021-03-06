from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from aiohttp import ClientSession
from time import time
from typing import Dict, TypedDict, Union, Optional
import ccxt
from starlette.responses import FileResponse 

app = FastAPI()
session = ClientSession()

@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

Number = Union[float, int]


class CEXPrice(TypedDict):
    amount: Number
    price: Number
    full_price: Number
    name: str


class Price(TypedDict):
    amount: Number
    price: Number
    full_price: Number
    exchanges: Dict[str, CEXPrice]


class Prices(TypedDict):
    bids: Price
    asks: Price


no_orders = {"asks": [], "bids": []}


def timed_cache(seconds=5 * 60, maxsize=50):
    def cached(f):
        values = {}

        async def wrapper(pair, *args):
            pair = tuple(pair)
            if pair in values:
                if values[pair]["ts"] > time():
                    return {**values[pair]["value"], "cached": True}
            value = await f(pair, *args)
            values[pair] = {"ts": time() + seconds, "value": value}
            if len(values) > maxsize:
                values.pop(min(values, key=lambda key: values[key]["ts"]))
            return {**value, "cached": False}

        return wrapper

    return cached


def gen_getter(exchange):
    @timed_cache(3600)
    async def getter(pair, is_reversed=False):
        try:
            result = exchange.fetch_l2_order_book("/".join(pair), 100)
        except:
            return {"bids": [], "asks": []}
        return {
            key: sorted(
                [
                    {
                        "price": order[0],
                        "amount": order[1],
                        "exchange": exchange.id,
                        "name": exchange.name,
                    }
                    for order in result[key]
                ],
                key=lambda x: x["amount"],
                reverse=key == "bids",
            )
            for key in ["asks", "bids"]
        }

    return getter


exchanges = [
    getattr(ccxt, x)()
    for x in [
        "binance",
        "bitfinex",
        "exmo",
        "ftx",
        "gateio",
        "hitbtc",
        "huobi",
        "kraken",
        "kucoin",
        "okcoin",
        "okex",
        "poloniex",
        "yobit",
    ]
]


getters = [gen_getter(cex) for cex in exchanges]


async def get_orders(pair):
    orders = {
        "bids": [],
        "asks": [],
    }
    for getter in getters:
        cex_orders = await getter(pair)
        orders["bids"].extend(cex_orders["bids"])
        orders["asks"].extend(cex_orders["asks"])
    return {
        key: sorted(
            orders[key], key=lambda order: order["price"], reverse=(key == "bids")
        )
        for key in orders
    }


async def fill_orders(pair, amount):
    filled = {
        "bids": [],
        "asks": [],
    }

    orders = await get_orders(pair)

    for key in filled:
        filled_amount = 0

        for order in orders[key]:
            if filled_amount + order["amount"] >= amount:
                filled[key].append({**order, "amount": amount - filled_amount})
                break
            filled_amount += order["amount"]
            filled[key].append(order)
    return filled


def calc_prices(order_list):
    amount = sum(order["amount"] for order in order_list)
    full_price = sum(order["price"] * order["amount"] for order in order_list)
    price = full_price / amount if amount else 0
    extra_fields = ["name"] if all("name" in order for order in order_list) else []
    return {
        "amount": amount,
        "price": price,
        "full_price": full_price,
        **{ef: order_list[0][ef] for ef in extra_fields if len(order_list) > 0},
    }


def compose_prices(order_list):
    exchanges = {}
    for order in order_list:
        if order["exchange"] not in exchanges:
            exchanges[order["exchange"]] = []
        exchanges[order["exchange"]].append(order)
    return {exchange: calc_prices(exchanges[exchange]) for exchange in exchanges}


async def get_prices(pair, amount):
    filled = await fill_orders(pair, amount)
    keys = ["bids", "asks"]
    return {
        key: {**calc_prices(filled[key]), "exchanges": compose_prices(filled[key])}
        for key in keys
    }


@app.get("/retrieve/{source}/{dest}", response_model=Prices)
async def main_method(source: str, dest: str, amount: int):
    """retrieve current price for source:destionation pair"""
    return await get_prices([source, dest], amount)


@app.get("/update/{source}/{dest}")
async def progress_bar(source: str, dest: str, i: int):
    if 0 <= i < len(exchanges):
        result = await getters[i]([source, dest])
        if result["cached"]:
            return {
                "next": None,
            }
        return {
            "next": {"name": exchanges[i + 1].name, "index": i + 1}
            if i + 1 < len(exchanges)
            else None,
        }
    raise HTTPException(
        status_code=400,
        detail=f"Invalid exchange index, use number from 0 to {len(exchanges) - 1}",
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
)


app.mount("/", StaticFiles(directory="static"), name="static")