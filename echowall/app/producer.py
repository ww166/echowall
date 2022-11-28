"""
    消息 生产者
"""
import functools
import time
import traceback

from echowall.app.conf import EWConfig
from echowall.app.message import EWMessage


class EWProducer:
    conf: EWConfig = None
    group_name: str = None
    _producer = None

    def __init__(self, group_name, conf: EWConfig):
        self.conf = conf
        self.group_name = group_name
        self._init_producer(group_name, conf)

    def __enter__(self):
        return self.__enter_impl__()

    def __exit__(self, exec_type, value, traceback):
        return self.__exit_impl__(exec_type, value, traceback)

    def start(self):
        return self._start()

    def shutdown(self):
        return self._shutdown()

    def _retry(foo):
        """
            自动重试
        :return:
        """
        def _get_retry_time(self, default_time=1, pass_args: list = None):
            _retry_time = default_time
            if self.conf.send_retry_backoff:
                if callable(self.conf.send_retry_backoff):
                    _retry_time = self.conf.send_retry_backoff(*pass_args)
                elif isinstance(self.conf.send_retry_backoff, int):
                    _retry_time = self.conf.send_retry_backoff
                else:
                    pass

            return _retry_time

        def magic(self, *args, **kwargs):
            _retry = self.conf.send_retry_count
            _cur_count = 0
            while (_retry is None or _retry <= 0) or (_cur_count < _retry):
                try:
                    return foo(self, *args, **kwargs)
                except Exception as e:
                    print(traceback.format_exc())
                    _cur_count += 1
                    if _retry is not None and _retry > 0:
                        _retry_time = _get_retry_time(self, pass_args=[_cur_count, _retry, args, kwargs])
                        time.sleep(_retry_time)

                    else:
                        raise e

        return magic

    @_retry
    def send_sync(self, msg: EWMessage) -> bool:
        msg.validate()
        return self._send_sync(msg.to_message())

    @_retry
    def send_oneway(self, msg: EWMessage) -> bool:
        msg.validate()
        return self.send_oneway(msg.to_message())

    @_retry
    def send_orderly_with_sharding_key(self, msg: EWMessage, sharding_key) -> bool:
        msg.validate()
        return self._send_orderly_with_sharding_key(msg.to_message(), sharding_key)

    def _init_producer(self, group_name: str = None, conf: EWConfig = None):
        raise NotImplementedError()

    def __enter_impl__(self):
        raise NotImplementedError()

    def __exit_impl__(self, exec_type, value, traceback):
        raise NotImplementedError()

    def _send_sync(self, msg) -> bool:
        raise NotImplementedError()

    def _send_oneway(self, msg) -> bool:
        raise NotImplementedError()

    def _send_orderly_with_sharding_key(self, msg, sharding_key) -> bool:
        raise NotImplementedError()

    def _start(self):
        raise NotImplementedError()

    def _shutdown(self):
        raise NotImplementedError()
