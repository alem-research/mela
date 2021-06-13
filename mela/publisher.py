import asyncio
import json
import logging
from typing import Optional, Union

import aio_pika
import aiormq

from mela.core.connectable import Connectable


class MelaPublisher(Connectable):
    CONNECTION_MODE = "write"

    def __init__(self, app: 'Mela', name):
        self.name = name
        self.log = logging.getLogger(app.name + '.' + self.name)
        super(MelaPublisher, self).__init__(app, self.name)
        self.exchange: Optional[aio_pika.RobustExchange] = None
        self.queue = asyncio.Queue(maxsize=1)
        self.default_routing_key = None
        self.decode = self.decode
        self.publishing_params = {}
        self.permanent_publishing_options = {}
        self.permanent_publishing_options.setdefault('content_type', "application/json")
        self.permanent_publishing_options.setdefault('content_encoding', "UTF-8")

    def __call__(self, *args, **kwargs):
        """
        THIS METHOD WILL BE DEPRECATED IN v1.1. DO NOT USE IT.
        """
        self.loop.create_task(self.publish(*args, **kwargs))

    def on_config_update(self):
        super().on_config_update()
        self.config.setdefault('connection', "default")
        self.config.setdefault('skip_unroutables', False)
        self.config.setdefault('exchange_type', "direct")
        if 'routing_key' not in self.config:
            raise KeyError(f"Routing key is not set for publisher {self.name}")
        self.default_routing_key = self.config['routing_key']
        if 'exchange' not in self.config:
            raise KeyError(f"Exchange is not set for publisher {self.name}")

    @staticmethod
    def decode(message):
        return bytes(json.dumps(message), encoding='utf-8')

    def set_decoder(self, func):
        self.decode = func

    async def ensure_exchange(self):
        await self.ensure_channel()
        try:
            self.exchange = await self.channel.declare_exchange(self.config['exchange'],
                                                                type=self.config['exchange_type'], durable=True)
        except Exception as e:
            self.log.warning("Error while declaring exchange")
            self.log.warning(e.__class__.__name__, e.args)

    async def publish(
            self,
            message: Union[dict, list, int, str, float],
            routing_key: Optional[str] = None,
            **options
    ):
        routing_key = routing_key or self.default_routing_key or ''
        options = {**self.permanent_publishing_options, **options}
        options.setdefault('content_type', "application/json")
        options.setdefault('content_encoding', "UTF-8")
        kwargs = {
            'message': aio_pika.Message(self.decode(message), **options),
            'routing_key': routing_key,
            **self.publishing_params
        }
        await self.queue.put(kwargs)

    async def prepare(self):
        self.log.info("preparing publisher")
        await self.ensure_connection()
        await self.ensure_exchange()

    async def publish_direct(
            self,
            message: Union[dict, list, int, str, float],
            routing_key: Optional[str] = None,
            **options
    ):
        self.log.info("Going to directly publish message")
        routing_key = routing_key or self.default_routing_key or ''
        options = {**self.permanent_publishing_options, **options}
        return await self.exchange.publish(
            aio_pika.Message(
                self.decode(message),
                **options
            ),
            routing_key=routing_key,
            **self.publishing_params
        )

    async def _publish(self, **kwargs):
        is_published = False
        resp = None
        while not is_published:
            try:
                resp: Optional[aiormq.types.ConfirmationFrameType, aiormq.types.DeliveredMessage] = \
                    await self.exchange.publish(**kwargs)
                is_published = isinstance(resp,
                                          aiormq.spec.Basic.Ack)  # or isinstance(resp, aiormq.types.DeliveredMessage)
                if not is_published:
                    error_message = f"Delivery response is {resp.delivery.__class__.__name__}." \
                                    f"It looks like message is unroutable"
                    if self.config['skip_unroutables']:
                        self.log.warning(error_message)
                        is_published = True
                    else:

                        raise TypeError(error_message)
            except aiormq.exceptions.ChannelInvalidStateError:
                await self.ensure_exchange()
            except Exception as e:
                self.log.exception("Unhandled exception: ")
        return resp

    async def wait(self):
        await self.queue.join()

    async def run(self):
        await self.ensure_connection()
        async with self.connection:
            await self.ensure_exchange()
            while True:
                message = await self.queue.get()
                self.log.debug(f"Got message should be published by {self.name}")
                await self._publish(**message)
                self.queue.task_done()