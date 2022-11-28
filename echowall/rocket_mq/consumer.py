"""
    rocketmq 消费者
"""
from echowall.app.conf import EWConfig
from echowall.app.consumer import EWConsumer
from echowall.utils.regex_tools import is_ip


class EWRocketMQConsumer(EWConsumer):
    def _init_consumer(self, group_name=None, conf: EWConfig = None):
        from echowall.rocket_mq.consumer_impl import CustomRocketMQConsumer
        if group_name is None:
            group_name = self.group_name

        if not group_name:
            raise ValueError('invalid group name')

        if conf is None:
            conf = self.conf

        if conf is None:
            raise ValueError('invalid config')

        self._consumer = CustomRocketMQConsumer(group_name, conf.orderly, conf.consumer_mode)

        if conf.host:
            if is_ip(conf.host):
                _sev = conf.host

                if conf.port:
                    _sev = ':'.join((_sev, str(conf.port)))

                self._consumer.set_name_server_address(_sev)
            else:
                _sev = conf.host
                if conf.port:
                    _sev = ':'.join((_sev, str(conf.port)))

                self._consumer.set_name_server_domain(_sev)

        if conf.access_key and conf.access_secret:
            self._consumer.set_session_credentials(conf.access_key, conf.access_secret, conf.channel)

        if conf.consumer_thread_count is not None:
            self._consumer.set_thread_count(conf.consumer_thread_count)

        if conf.instance_name:
            self._consumer.set_instance_name(conf.instance_name)

        if conf.consumer_log_path:
            self._consumer.set_log_path(conf.consumer_log_path)

        if conf.consumer_log_file_num and conf.consumer_log_file_size:
            self._consumer.set_log_file_num_and_size(conf.consumer_log_file_num, conf.consumer_log_file_size)

        if conf.consumer_log_level:
            self._consumer.set_log_level(conf.consumer_log_level)

        return self._consumer

    def __enter_impl__(self):
        return self._consumer.__enter__()

    def __exit_impl__(self, exec_type, value, traceback):
        return self._consumer.__exit__(exec_type, value, traceback)

    def _start(self):
        return self._consumer.start()

    def _shutdown(self):
        return self._consumer.shutdown()

    def _subscribe(self, topic, callback, expression='*', *args, **kwargs):
        return self._consumer.subscribe(topic, callback, expression)
