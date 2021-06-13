import asyncio
from typing import Optional

import aio_pika
import aiormq

from mela.core.loggable import Loggable


class ConnectionRegistry(Loggable):
    connection_lock = asyncio.Lock()

    def __init__(self, app, name):
        super().__init__(app, name)
        self._registry = {}
        self.loop = app.loop

    async def get(self, name, mode='read'):
        await self.connection_lock.acquire()
        connection_name = f"{name}_{mode}"
        if connection_name not in self._registry:
            self.log.debug(f"Making new connection {name} with mode {mode}")
            connection = await self.make_connection(name, mode)
            self._registry[connection_name] = connection
        self.connection_lock.release()
        return self._registry[connection_name]

    async def make_connection(self, name, mode):
        config = self.app.config['connections'].get(name, {})
        if 'url' not in config and 'host' not in config:
            raise KeyError(f"Connection {name} is not configured")
        if 'username' in config and 'login' not in config:
            config.setdefault('login', config['username'])
        connection: Optional[aio_pika.RobustConnection] = None
        while connection is None:
            try:
                connection = await aio_pika.connect_robust(
                    **config,
                    loop=self.loop,
                    client_properties={
                        'connection_name': f"{self.app.name}_{name}_{mode}"
                    }
                )
                # await connection.connected.wait()
            except ConnectionError:
                self.log.warning("Connection refused, trying again")
            except aiormq.exceptions.IncompatibleProtocolError:
                self.log.warning("aiormq.exceptions.IncompatibleProtocolError")
            except Exception as e:
                self.log.warning("Unhandled exception while connecting: %s" % e.__class__.__name__)
            finally:
                await asyncio.sleep(1)
        return connection
