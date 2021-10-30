from fastapi import FastAPI
from aiohttp import ClientSession
from time import time

app = FastAPI()
session = ClientSession()


def timed_cache(seconds, maxsize=25):
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


@timed_cache(60)
async def get_binance(pair):
    endpoint = "https://api3.binance.com"
    async with session.get(endpoint + "/api/v3/depth", params={"symbol": "".join(pair), "limit": 1000}) as resp:
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
            ]
            for key in keys
            for data in [result[keys[key]]]
        }


getters = [get_binance]


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


@app.get("/{source}:{dest}")
async def hello(source: str, dest: str, amount: int):
    return await get_prices([source, dest], amount)
