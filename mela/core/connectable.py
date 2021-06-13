from typing import Optional

import aio_pika

from mela.core.loggable import Loggable


class Connectable(Loggable):
    CONNECTION_MODE = 'read'

    def __init__(self, registry, name):
        super().__init__(name)
        self.registry = registry
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None

    async def ensure_connection(self):
        if self.connection is None:
            try:
                self.connection = await self.registry.get(self.config.get('connection', "default"),
                                                          mode=self.CONNECTION_MODE)
            except Exception as e:
                self.log.exception("Unhandled exception while connecting: ")

    async def ensure_channel(self):
        await self.ensure_connection()
        if self.channel is None:
            try:
                self.channel = await self.connection.channel()
            except Exception as e:
                self.log.warning("Error while creating channel", exc_info=True)
        elif self.channel.is_closed:
            await self.channel.reopen()
