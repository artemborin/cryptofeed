'''
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging

from cryptofeed.backends.backend import BackendCallback
from cryptofeed.backends.socket import SocketCallback
from cryptofeed.defines import ASK, BID
from questdb.ingress import Sender, TimestampNanos, TimestampMicros


LOG = logging.getLogger('feedhandler')


class QuestCallback(SocketCallback):
    def __init__(self, host='127.0.0.1', port=9009, key=None, **kwargs):
        super().__init__(f"tcp://{host}", port=port, **kwargs)
        self.host = host
        self.key = key if key else self.default_key
        self.numeric_type = float
        self.none_to = None
        self.running = True

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                conf = f'http::addr={self.host}:9001;'
                with Sender.from_conf(conf) as sender:
                    for update in updates:
                        sender.row(update['table'], symbols=update['symbols'], columns=update['columns'], at=update['at'])

    async def write(self, data):
        d = self.format(data)
        timestamp = data["timestamp"]
        received_timestamp_int = int(data["receipt_timestamp"] * 1_000_000)
        timestamp_int = int(timestamp * 1_000_000_000) if timestamp is not None else received_timestamp_int * 1000
        update = f'{self.key}-{data["exchange"]},symbol={data["symbol"]} {d},receipt_timestamp={received_timestamp_int}t {timestamp_int}'
        await self.queue.put(update)

    def format(self, data):
        ret = []
        for key, value in data.items():
            if key in {'timestamp', 'exchange', 'symbol', 'receipt_timestamp'}:
                continue
            if isinstance(value, str):
                ret.append(f'{key}="{value}"')
            else:
                ret.append(f'{key}={value}')
        return ','.join(ret)


class TradeQuest(QuestCallback, BackendCallback):
    default_key = 'trades'

    async def write(self, data):
        timestamp = data["timestamp"]
        received_timestamp_int = int(data["receipt_timestamp"] * 1_000_000_000)
        timestamp_int = int(timestamp * 1_000_000) if timestamp is not None else None
        update = {
            'table': self.key,
            'symbols': {'exchange': data["exchange"], 'symbol': data["symbol"], 'side': data["side"], 'type': data["type"]},
            'columns': {'price': float(data["price"]), 'amount': float(data["amount"])},
            'at': TimestampNanos(received_timestamp_int)
        }
        if data["id"] is not None and data["id"].isdigit():
            update['columns']['id'] = int(data["id"])
        if timestamp_int is not None:
            update['columns']['venue_timestamp'] = TimestampMicros(timestamp_int)
        await self.queue.put(update)


class FundingQuest(QuestCallback, BackendCallback):
    default_key = 'funding'


class BookQuest(QuestCallback):
    default_key = 'book'

    def __init__(self, *args, depth=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.depth = depth

    async def __call__(self, book, receipt_timestamp: float):
        vals = ','.join([f"bid_{i}_price={book.book.bids.index(i)[0]},bid_{i}_size={book.book.bids.index(i)[1]}" for i in range(self.depth)] + [f"ask_{i}_price={book.book.asks.index(i)[0]},ask_{i}_size={book.book.asks.index(i)[1]}" for i in range(self.depth)])
        timestamp = book.timestamp
        receipt_timestamp_int = int(receipt_timestamp * 1_000_000)
        timestamp_int = int(timestamp * 1_000_000_000) if timestamp is not None else receipt_timestamp_int * 1000
        update = f'{self.key}-{book.exchange},symbol={book.symbol} {vals},receipt_timestamp={receipt_timestamp_int}t {timestamp_int}'
        await self.queue.put(update)

class BookDeltaL2Quest(QuestCallback):
    default_key = 'book'

    def __init__(self, *args, depth=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.depth = depth

    def get_string_from_delta(self,book, side, price, size, venue_timestamp_int, receipt_timestamp_int):
        update = {
            'table': self.key,
            'symbols': {'exchange': book.exchange, 'symbol': book.symbol, 'side': side},
            'columns': {'price': float(price), 'size': float(size)},
            'at': TimestampNanos(receipt_timestamp_int)
        }
        if venue_timestamp_int is not None:
            update['columns']['venue_timestamp'] = TimestampMicros(venue_timestamp_int)
        return update
        
    async def __call__(self, book, receipt_timestamp: float):
        timestamp = book.timestamp
        receipt_timestamp_int = int(receipt_timestamp * 1_000_000_000)
        timestamp_int = int(timestamp * 1_000_000) if timestamp is not None else None
        updates = None
        if book.delta:
            updates = [self.get_string_from_delta(book, side, price, size, timestamp_int, receipt_timestamp_int) for side in (BID, ASK) for price, size in book.delta[side]]
        else:
            updates = [self.get_string_from_delta(book, side, price, size, timestamp_int, receipt_timestamp_int) for side in (BID, ASK) for price, size in book.book[side].to_dict().items()]
        for update in updates:
            await self.queue.put(update)

class BookDeltaL3Quest(QuestCallback):
    default_key = 'book'

    def __init__(self, *args, depth=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.depth = depth

    def get_string_from_delta(self,book, side, order_id, price, size, venue_timestamp_int, receipt_timestamp_int):
        update = {
            'table': self.key,
            'symbols': {'exchange': book.exchange, 'symbol': book.symbol, 'side': side},
            'columns': {'order_id': order_id, 'price': float(price), 'size': float(size)},
            'at': TimestampNanos(receipt_timestamp_int)
        }
        if venue_timestamp_int is not None:
            update['columns']['venue_timestamp'] = TimestampMicros(venue_timestamp_int)
        return update
        
    async def __call__(self, book, receipt_timestamp: float):
        timestamp = book.timestamp
        receipt_timestamp_int = int(receipt_timestamp * 1_000_000_000)
        timestamp_int = int(timestamp * 1_000_000) if timestamp is not None else None
        updates = None
        if book.delta:
            updates = [self.get_string_from_delta(book, side, order_id, price, size, timestamp_int, receipt_timestamp_int) for side in (BID, ASK) for order_id, price, size in book.delta[side]]
        else:
            updates = [self.get_string_from_delta(book, side, order_id, price, size, timestamp_int, receipt_timestamp_int) for side in (BID, ASK) for price, d in book.book[side].to_dict().items() for order_id, size in d.items()]
        for update in updates:
            await self.queue.put(update)

class TickerQuest(QuestCallback, BackendCallback):
    default_key = 'ticker'


class OpenInterestQuest(QuestCallback, BackendCallback):
    default_key = 'open_interest'


class LiquidationsQuest(QuestCallback, BackendCallback):
    default_key = 'liquidations'


class CandlesQuest(QuestCallback, BackendCallback):
    default_key = 'candles'

    async def write(self, data):
        timestamp = data["timestamp"]
        timestamp_str = f',timestamp={int(timestamp * 1_000_000_000)}i' if timestamp is not None else ''
        trades = f',trades={data["trades"]},' if data['trades'] else ','
        update = f'{self.key}-{data["exchange"]},symbol={data["symbol"]},interval={data["interval"]} start={data["start"]},stop={data["stop"]}{trades}open={data["open"]},close={data["close"]},high={data["high"]},low={data["low"]},volume={data["volume"]}{timestamp_str},receipt_timestamp={int(data["receipt_timestamp"]) * 1_000_000}t {int(data["receipt_timestamp"] * 1_000_000_000)}'
        await self.queue.put(update)


class OrderInfoQuest(QuestCallback, BackendCallback):
    default_key = 'order_info'


class TransactionsQuest(QuestCallback, BackendCallback):
    default_key = 'transactions'


class BalancesQuest(QuestCallback, BackendCallback):
    default_key = 'balances'


class FillsQuest(QuestCallback, BackendCallback):
    default_key = 'fills'
