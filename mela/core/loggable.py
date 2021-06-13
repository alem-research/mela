import logging

from mela.core.configurable import Configurable


class Loggable(Configurable):

    def __init__(self, name=None):
        super(Loggable, self).__init__(name=name)
        self.log = None
        self.set_logger(logging.getLogger(self.name))

    def set_logger(self, logger: 'logging.Logger'):
        self.log = logger
        self.log.setLevel(logging.INFO)

    def on_config_update(self):
        super().on_config_update()
        if 'log' in self.config:
            if 'name' in self.config['log']:
                self.log = logging.getLogger(self.config['log']['name'])
