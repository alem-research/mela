import logging

import asyncio

from mela import MelaPublisher, MelaConsumer, MelaService
from mela.connection_registry import ConnectionRegistry
from mela.consumer import MelaConsumer
from mela.core.connectable import Connectable
from mela.core.loggable import Loggable
from mela.publisher import MelaPublisher
from mela.service import MelaService

try:
    import ujson as json
except ImportError:
    import json

logging.basicConfig(format='%(name)s\t%(asctime)s\t%(levelname)s\t%(message)s', level=logging.INFO)


class Mela(Loggable):

    def __init__(self, name):
        super(Mela, self).__init__(self, name)
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._runnables = []
        self._connection_registry = ConnectionRegistry(self, 'connections')
        self._results = None
        self.services = {}
        self.publishers = {}
        self.consumers = {}

    def on_config_update(self):
        self.config.setdefault('publishers', {})
        self.config.setdefault('producers', {})  # TODO DEPRECATION remove in v1.1
        self.config.setdefault('consumers', {})
        self.config.setdefault('services', {})
        self.config.setdefault('connections', {})
        if 'connection' in self.config and not self.config['connections']:
            self.config['connections'] = {'default': self.config['connection']}

    def publisher(self, name="default", options=None) -> 'MelaPublisher':
        if options is None:
            options = {}
        if options:
            self.log.warning("DEPRECATION: `Mela.publisher` will not "
                             "take options since v1.1. Use config file.")
        self.ensure_configured()
        if name not in self.config['publishers'] and name not in self.config['producers']:
            # TODO DEPRECATION v1.1 remove `producers`
            if 'publisher' in self.config:
                name = 'default'
                self.config['publishers'][name] = self.config['publisher']
            else:
                raise KeyError(f"No config found for publisher {name}")
        publisher = MelaPublisher(self, name)
        publisher.configure({**(self.config['publishers'].get(name) or {}), **(self.config['producers'].get(name) or {}), **options})
        self.register(publisher)
        return publisher

    def producer(self, name, options=None):
        """
        DEPRECATED
        Will be removed in v.1.1

        Only for legacy usage
        """
        self.log.warning("DEPRECATION: `Mela.producer` is replaced with `Mela.publisher`")

        def decorator(*args, **kwargs):
            return self.publisher(name, options)
        return decorator

    def consumer(self, name):
        def decorator(func):
            consumer = MelaConsumer(self, name)
            consumer.configure(self.config['consumers'][name])
            consumer.set_processor(func)
            self.register(consumer)
            return consumer

        return decorator

    def service(self, name, producer_name=None, options_consumer=None, options_publisher=None):
        if options_publisher is None:
            options_publisher = {}
        if options_consumer is None:
            options_consumer = {}
        if producer_name:
            self.log.warning("DEPRECATION: `Mela.service` will use only one argument, service name. "
                             "Update your config format.")
        if options_consumer or options_publisher:
            self.log.warning("DEPRECATION: `Mela.service` will not "
                             "take options since v1.1. Use config file.")

        def decorator(func):
            service = MelaService(self, name)
            if producer_name:
                self.config['services'][name] = {}
                self.config['services'][name]['consumer'] = {**self.config['consumers'][name], **options_consumer}
                self.config['services'][name]['publisher'] = {**self.config['producers'][producer_name],
                                                              **options_publisher}
            service.configure(self.config['services'][name])
            service.set_processor(func)
            self.register(service)
            return service

        return decorator

    def register(self, other):
        if isinstance(other, MelaService):
            self.services[other.name] = other
        elif isinstance(other, MelaConsumer):
            self.consumers[other.name] = other
        elif isinstance(other, MelaPublisher):
            self.publishers[other.name] = other
        self._runnables.append(other)

    @property
    def connections(self):
        return self._connection_registry

    def run(self, coro=None):
        for runnable in self._runnables:
            self.loop.create_task(runnable.run())

        self.log.info("Running app...")

        if coro:
            self.loop.run_until_complete(coro)
        else:
            self.loop.run_forever()


