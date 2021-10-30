from fastapi import FastAPI
from aiohttp import ClientSession
from time import time
from typing import Dict, TypedDict, Union, Optional

app = FastAPI()
session = ClientSession()

Number = Union[float, int]


class CEXPrice(TypedDict):
    amount: Number
    price: Number
    full_price: Number


class Price(TypedDict):
    amount: Number
    price: Number
    full_price: Number
    exchanges: Dict[str, CEXPrice]


class Prices(TypedDict):
    bid: Price
    ask: Price



no_orders = {
    "ask": [],
    "bid": []
}



def timed_cache(seconds=5 * 60, maxsize=25):
    def cached(f):
        values = {}
        async def wrapper(pair):
            print(values.keys())
            pair = tuple(pair)
            if pair in values:
                if values[pair]["ts"] > time():
                    print(f"USING CACHED {pair}")
                    return values[pair]["value"]
            value = await f(pair)
            values[pair] = {"ts": time() + seconds, "value": value}
            print(f"CACHING {pair}")
            if len(values) > maxsize:
                print(f"TRIMMING CACHE {pair}")
                values.pop(min(values, key=lambda key: values[key]["ts"]))
            return value
        return wrapper
    return cached


@timed_cache()
async def get_binance(pair):
    endpoint = "https://api3.binance.com/api/v3/depth"
    async with session.get(endpoint, params={"symbol": "".join(pair), "limit": 100}) as resp:
        result = await resp.json()
        keys = {"bid": "bids", "ask": "asks"}
        return {
            key: [
                {
                    "price": float(order[0]),
                    "amount": float(order[1]),
                    "exchange": "binance",
                }
                for order in data
            ] if data else []
            for key in keys
            for data in [result.get(keys[key])]
        }


@timed_cache()
async def get_gate(pair):
    endpoint = "https://api.gateio.ws/api/v4/spot/order_book"
    async with session.get(endpoint, params={"currency_pair": "_".join(pair), "limit": 100}) as resp:
        result = await resp.json()
        keys = {"bid": "bids", "ask": "asks"}
        return {
            key: [
                {
                    "price": float(order[0]),
                    "amount": float(order[1]),
                    "exchange": "gateio",
                }
                for order in data
            ]
            for key in keys
            for data in [result[keys[key]]]
        }


@timed_cache()
async def get_kraken(pair):
    endpoint = "https://api.kraken.com/0/public/Depth"
    async with session.get(endpoint, params={"pair": "".join(pair), "count": 100}) as resp:
        result = (await resp.json()).get("result")
        if not result:
            return no_orders
        keys = {"bid": "bids", "ask": "asks"}
        pair = [*result.keys()][0]
        print(result[pair])
        return {
            key: [
                {
                    "price": float(order[0]),
                    "amount": float(order[1]),
                    "exchange": "kraken",
                }
                for order in data
            ]
            for key in keys
            for data in [result[pair][keys[key]]]
        }


getters = [get_binance, get_gate, get_kraken]


async def get_orders(pair):
    orders = {
        "bid": [],
        "ask": [], 
    }
    for getter in getters:
        cex_orders = await getter(pair)
        orders["bid"].extend(cex_orders["bid"])
        orders["ask"].extend(cex_orders["ask"])
    return {
        key: sorted(
            orders[key], 
            key=lambda order: order["price"], 
            reverse=(key == "bid")
        )
        for key in orders
    }


async def fill_orders(pair, amount):
    filled = {
        "bid": [],
        "ask": [],
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
    price = full_price / amount
    return {"amount": amount, "price": price, "full_price": full_price}


def compose_prices(order_list):
    exchanges = {}
    for order in order_list:
        if order["exchange"] not in exchanges:
            exchanges[order["exchange"]] = []
        exchanges[order["exchange"]].append(order)
    return {exchange: calc_prices(exchanges[exchange]) for exchange in exchanges}


async def get_prices(pair, amount):
    filled = await fill_orders(pair, amount)
    keys = ["bid", "ask"]
    return {
        key: {
            **calc_prices(filled[key]),
            "exchanges": compose_prices(filled[key])
        }
        for key in keys
    }


@app.get("/{source}:{dest}", response_model=Prices)
async def main_method(source: str, dest: str, amount: int):
    """retrieve current price for source:destionation pair"""
    print(await get_prices([source, dest], amount))
    return await get_prices([source, dest], amount)
