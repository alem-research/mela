import logging
from typing import Optional
from asyncio import Event

import envyaml


class Configurable:

    def __init__(self, name=None):
        self.config: Optional[dict] = None
        self.configured = Event()
        self.name = name

    def configure(self, config: dict):
        self.config = config
        self.configured.set()
        # self.on_config_update()
        # self.ensure_configured()

    def configure_from_yaml(self, filename):
        env = envyaml.EnvYAML(filename)
        self.configure(dict(env))

    def read_config_yaml(self, filename):
        """
        DEPRECATED
        Will be removed in v.1.1

        Only for legacy usage
        """
        logging.warning("DEPRECATION: `Configurable.read_config_yaml` will be "
                        "replaced with `Configurable.configure_from_yaml` in v1.1")
        self.configure_from_yaml(filename)

    async def on_config_update(self):
        pass

    def is_configured(self):
        return self.configured.is_set()

    def initialize(self):
        pass

    def ensure_configured(self):
        if not self.is_configured():
            raise Exception(f"No config provided for {self}")

    async def run(self):
        await self.configured.wait()
        self.initialize()
