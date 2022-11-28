"""
    消息队列的消费者和生产者通用客户端
"""
import time
import typing

from echowall.app.conf import EWConfig
from echowall.app.consts import EWConsumeStatus
from echowall.app.consumer import EWConsumer
from echowall.app.message import EWMessage
from echowall.app.producer import EWProducer


class EWClient:
    conf: EWConfig = None

    _producers: typing.Dict[str, EWProducer] = {}
    _consumers: typing.Dict[str, EWConsumer] = {}
    _echo_wall = None

    def __init__(self,
                 conf: EWConfig):
        self.conf = conf
        pass

    def new_message(self,
                    topic: str,
                    conf: EWConfig = None) -> typing.Optional[EWMessage]:
        """
            构建新的消息
        :param topic:
        :param conf:
        :return:
        """
        if conf is None:
            conf = self.conf

        if conf is None:
            raise ValueError('invalid config')

        from echowall.factory.message import gen_message
        return gen_message(topic, conf)

    def start_producer(self,
                       group_name: str,
                       *args, **kwargs):
        """
            初始化生产者
        :param group_name:
        :param args:
        :param kwargs:
        :return:
        """
        from echowall.factory.producer import gen_producer

        if group_name in self._producers:
            return

        _producer = gen_producer(group_name, self.conf)
        self._producers[group_name] = _producer
        _producer.start()

    def send_sync_message(self,
                          group_name: str,
                          msg: EWMessage,
                          record_kwargs=None,
                          auto_create_group_producer=True,
                          *args, **kwargs) -> (bool, bool):
        """
            同步发送消息
        :param group_name: 消息分组名称
        :param msg: 消息
        :param record_kwargs: 传递给落库函数
        :param auto_create_group_producer: 如果 group name 尚未配置 producer，自动创建对应的producer
        :param args:
        :param kwargs:
        :return: 消息发送是否成功，落库是否成功
        """
        if group_name not in self._producers:
            if auto_create_group_producer:
                self.start_producer(group_name)
            else:
                raise KeyError('Invalid group name.')

        msg.latest_send_timestamp = int(time.time())

        record_flag = True

        producer = self._producers[group_name]

        if self.conf.record_func and not self.conf.record_only_success:
            self.conf.record_func(group_name, msg, record_kwargs)

        ret = producer.send_sync(msg)

        try:
            if ret and self.conf.record_func and self.conf.record_only_success:
                self.conf.record_func(group_name, msg, record_kwargs)
        except Exception as e:
            record_flag = False

        return ret, record_flag

    def shutdown_producer(self, group_name):
        """
            关闭生产者
        :param group_name:
        :return:
        """
        if group_name not in self._producers:
            return

        producer = self._producers[group_name]
        producer.shutdown()

    def create_consumer(self,
                        group_name: str,
                        topic: str,
                        callback: typing.Callable[[EWMessage], EWConsumeStatus],
                        tag: str = '*'):
        """
            构建消费者
        :param group_name:
        :param topic:
        :param callback:
        :param tag:
        :return:
        """
        from echowall.factory.consumer import gen_consumer

        if group_name in self._consumers:
            consumer = self._consumers[group_name]
        else:
            consumer = gen_consumer(group_name, self.conf)
            self._consumers[group_name] = consumer

        consumer.subscribe(topic, callback, tag)

    def start_consumers(self):
        """
            批量启动消费者
        :return:
        """
        for _, consumer in self._consumers.items():
            consumer.start()

    def start_consumer(self, group_name, *args, **kwargs):
        """
            启动单个消费者
        :param group_name:
        :param args:
        :param kwargs:
        :return:
        """
        if group_name not in self._consumers:
            raise KeyError('Group name: \'{}\' does not exist'.format(group_name))

        consumer = self._consumers[group_name]
        consumer.start()

    def shutdown_consumers(self):
        """
            批量关闭消费者
        :return:
        """
        for _, consumer in self._consumers.items():
            consumer.shutdown()

    def shutdown_consumer(self, group_name, *args, **kwargs):
        """
            关闭单个消费者
        :param group_name:
        :param args:
        :param kwargs:
        :return:
        """
        if group_name in self._consumers:
            consumer = self._consumers[group_name]
            consumer.shutdown()

    def bind_echo_wall(self, echo_wall):
        self._echo_wall = echo_wall

    def set_consumers_with_echo_wall(self, settings: list):
        if not self._echo_wall:
            raise ValueError('Echo wall is None, please call bind_echo_wall first')

        _echo_wall = self._echo_wall
        _pc_callbacks = _echo_wall.pc_callbacks
        if _pc_callbacks == {}:
            raise ValueError('No callback functions has been registered yet.')

        for setting in settings:
            group_name, topic, tag, func_name = setting
            pc_callback = _pc_callbacks.get(func_name, None)
            if not pc_callback:
                raise KeyError('Callback function \'{}\' does not exist'.format(func_name))
            self.create_consumer(group_name, topic, pc_callback.run, tag)
