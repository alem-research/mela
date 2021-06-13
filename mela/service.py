import inspect

import aio_pika

from mela.core.loggable import Loggable
from mela.publisher import MelaPublisher
from mela.consumer import MelaConsumer


class MelaService(Loggable):

    async def __response_processor_for_generator(self, response):
        for obj in response:
            await self.publish(obj)

    async def __response_processor_for_async_generator(self, response):
        async for obj in response:
            await self.publish(obj)

    async def __response_processor_for_function(self, response):
        await self.publish(response)

    def __init__(self, app, name):
        consumer_name = name + '_consumer'
        publisher_name = name + '_publisher'
        super().__init__(app, name)
        self.publisher = MelaPublisher(app, publisher_name)
        self.consumer = MelaConsumer(app, consumer_name)
        self.response_processor = None

    async def publish(self, message):
        if isinstance(message, tuple):
            await self.publisher.publish(message[0], **message[1])
        else:
            await self.publisher.publish(message)

    async def on_message_processed(self, response, message: aio_pika.IncomingMessage):
        if response is not None:
            await self.response_processor(response)
            await self.publisher.wait()
        if not message.processed:
            message.ack()

    def on_config_update(self):
        connection_name = self.config.get("connection", "default")
        if isinstance(self.config['publisher'], str):
            self.config['publisher'] = self.app.config['publishers'][self.config['publisher']]
        if isinstance(self.config['consumer'], str):
            self.config['consumer'] = self.app.config['consumers'][self.config['consumer']]
        self.config['publisher'].setdefault('connection', connection_name)
        self.config['consumer'].setdefault('connection', connection_name)
        self.publisher.configure(self.config['publisher'])
        self.consumer.configure(self.config['consumer'])
        self.consumer.set_on_message_processed(self.on_message_processed)

    def set_processor(self, func):
        self.consumer.set_processor(func)
        if inspect.isgeneratorfunction(self.consumer.process):
            self.response_processor = self.__response_processor_for_generator
        elif inspect.isasyncgenfunction(self.consumer.process):
            self.response_processor = self.__response_processor_for_async_generator
        else:
            self.response_processor = self.__response_processor_for_function

    async def run(self):
        self.log.info(f"Connecting {self.name} service")
        self.app.loop.create_task(self.publisher.run())
        self.app.loop.create_task(self.consumer.run())

    async def cancel(self):
        await self.consumer.cancel()

    def __call__(self, *args, **kwds):
        """Method that allows calling a function that is used for
        service creation.
        """
        return self.consumer.process(*args, **kwds)
