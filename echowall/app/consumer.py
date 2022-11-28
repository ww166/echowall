"""
    消费者
"""
from echowall.app.conf import EWConfig


class EWConsumer(object):
    conf: EWConfig = None
    group_name: str = None
    _consumer = None

    def __init__(self, group_name, conf: EWConfig):
        self.conf = conf
        self.group_name = group_name

        super(EWConsumer, self).__init__()

        self._init_consumer(group_name, conf)

    def _init_consumer(self, group_name=None, conf: EWConfig = None):
        raise NotImplementedError()

    def __enter__(self):
        return self.__enter_impl__()

    def __exit__(self, exec_type, value, traceback):
        return self.__exit_impl__(exec_type, value, traceback)

    def start(self):
        return self._start()

    def shutdown(self):
        return self._shutdown()

    def subscribe(self, topic, callback, expression='*', *args, **kwargs):
        return self._subscribe(topic, callback, expression, *args, **kwargs)

    def __enter_impl__(self):
        raise NotImplementedError()

    def __exit_impl__(self, exec_type, value, traceback):
        raise NotImplementedError()

    def _start(self):
        raise NotImplementedError()

    def _shutdown(self):
        raise NotImplementedError()

    def _subscribe(self, topic, callback, expression='*', *args, **kwargs):
        raise NotImplementedError()
