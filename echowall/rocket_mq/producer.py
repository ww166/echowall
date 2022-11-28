"""
    rocketmq 生产者
"""
from echowall.app.conf import EWConfig
from echowall.app.producer import EWProducer

from echowall.utils.regex_tools import is_ip


class EWRocketMQProducer(EWProducer):
    _producer = None

    def _init_producer(self, group_name: str = None, conf: EWConfig = None):
        if conf is None:
            conf = self.conf

        if conf is None:
            raise ValueError('invalid config for EWProducer')

        if group_name is None:
            group_name = self.group_name

        if group_name is None:
            raise ValueError('invalid group name for EWProducer')

        self._producer = None

        from echowall.rocket_mq.producer_impl import CustomRocketMQProducer
        self._producer = CustomRocketMQProducer(
            self.group_name,
            conf.orderly,
            conf.timeout,
            conf.compress_level,
            conf.max_message_size
        )
        self._producer.set_group(group_name)

        if conf.host:
            if is_ip(conf.host):
                _sev = conf.host

                if conf.port:
                    _sev = ':'.join((_sev, str(conf.port)))

                self._producer.set_name_server_address(_sev)
            else:
                _sev = conf.host
                if conf.port:
                    _sev = ':'.join((_sev, str(conf.port)))

                self._producer.set_name_server_domain(_sev)

        if conf.access_key and conf.access_secret:
            self._producer.set_session_credentials(conf.access_key, conf.access_secret, conf.channel)

        if conf.producer_log_path:
            self._producer.set_log_path(conf.producer_log_path)

        if conf.producer_log_file_num and conf.producer_log_file_size:
            self._producer.set_log_file_num_and_size(conf.producer_log_file_num, conf.producer_log_file_size)

        if conf.producer_log_level:
            self._producer.set_log_level(conf.producer_log_level)

        return self._producer

    def __enter_impl__(self):
        self._producer.__enter__()

    def __exit_impl__(self, exec_type, value, traceback):
        self._producer.__exit__(exec_type, value, traceback)

    def _is_send_successful(self, send_result) -> bool:
        from rocketmq.client import SendStatus
        if send_result:
            return send_result.status == SendStatus.OK
        return False

    def _send_sync(self, msg) -> bool:
        ret = self._producer.send_sync(msg)
        return self._is_send_successful(ret)

    def _send_oneway(self, msg) -> bool:
        ret = self._producer.send_oneway(msg)
        return self._is_send_successful(ret)

    def _send_orderly_with_sharding_key(self, msg, sharding_key) -> bool:
        ret = self._producer.send_orderly_with_sharding_key(msg, sharding_key)
        return self._is_send_successful(ret)

    def _start(self):
        self._producer.start()

    def _shutdown(self):
        self._producer.shutdown()
