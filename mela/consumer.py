import asyncio
import inspect
import json
import logging
from typing import Optional, Callable

import aio_pika

from mela.core.connectable import Connectable


class MelaConsumer(Connectable):

    def __init__(self, app, name):
        super().__init__(app, name)
        self.connection = None
        self.connection_established = asyncio.Event()
        self.log = logging.Logger("MelaConsumer", level=logging.INFO)
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.exchange: Optional[aio_pika.RobustExchange] = None
        self.queue: Optional[aio_pika.RobustQueue] = None
        self.encode: Optional[Callable] = self.encode
        self.process: Optional[Callable] = self.process
        self.on_message_processed: Optional[Callable] = self.on_message_processed
        self.queue_iterator: Optional[aio_pika.queue.QueueIterator] = None

    def on_config_update(self):
        super().on_config_update()
        if 'queue' not in self.config:
            raise KeyError(f"No queue found in config for {self.name}")
        if 'exchange' not in self.config:
            raise KeyError(f"No exchange found in config for {self.name}")
        if 'routing_key' not in self.config:
            raise KeyError(f"No routing key found in config for {self.name}")
        self.config.setdefault('prefetch_count', 1)
        self.config.setdefault('exchange_type', "direct")

    async def ensure_channel(self):
        await super().ensure_channel()
        await self.channel.set_qos(prefetch_count=self.config['prefetch_count'])

    async def ensure_exchange(self):
        if self.exchange is None:
            await self.ensure_channel()
            try:
                self.exchange = await self.channel.declare_exchange(self.config['exchange'],
                                                                    type=self.config['exchange_type'], durable=True)
            except Exception as e:
                self.log.warning("Error while declaring exchange")
                self.log.warning(e.__class__.__name__, e.args)

    async def ensure_queue(self):
        if self.queue is None:
            await self.ensure_channel()
            try:
                args = {}
                if 'dead_letter_exchange' in self.config:
                    await self.channel.declare_exchange(self.config['dead_letter_exchange'], durable=True)
                    args['x-dead-letter-exchange'] = self.config['dead_letter_exchange']
                    if 'dead_letter_routing_key' in self.config and self.config['dead_letter_routing_key']:
                        args['x-dead-letter-routing-key'] = self.config['dead_letter_routing_key']
                self.queue = await self.channel.declare_queue(self.config['queue'], durable=True, arguments=args)
            except Exception as e:
                self.log.warning("Error while declaring queue")
                self.log.warning(e.__class__.__name__, e.args)

    async def ensure_binding(self):
        await self.ensure_exchange()
        await self.ensure_queue()
        try:
            await self.queue.bind(self.config['exchange'], routing_key=self.config['routing_key'])
        except Exception as e:
            self.log.warning("Error while declaring queue")
            self.log.warning(e.__class__.__name__, e.args)

    async def run(self):
        await self.ensure_connection()

        async with self.connection:
            await self.ensure_binding()

            async with self.queue.iterator() as queue_iter:
                self.queue_iterator = queue_iter
                async for message in queue_iter:  # type: aio_pika.IncomingMessage
                    async with message.process(ignore_processed=True):
                        body = self.encode(message.body)
                        if inspect.iscoroutinefunction(self.process):
                            resp = await self.process(body, message)
                        else:
                            resp = self.process(body, message)
                        await self.on_message_processed(resp, message)

    @staticmethod
    async def on_message_processed(response, message):
        pass

    @staticmethod
    def encode(message_body: bytes):
        return json.loads(message_body)

    @staticmethod
    async def process(body, message):
        raise NotImplementedError

    def set_processor(self, func):
        self.process = func

    def set_on_message_processed(self, func):
        self.on_message_processed = func

    def set_encoder(self, func):
        self.encode = func

    async def cancel(self):
        await self.queue_iterator.close()
