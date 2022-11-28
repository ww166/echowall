"""
    rocketmq 消息
"""

import base64
import pickle
import traceback

from echowall.app.exceptions import UnknownMessage
from echowall.app.message import EWMessage


class EWRocketMQMessage(EWMessage):
    """
        系统数据
    """
    __sys_members = ('topic', 'keys', 'tags', 'body', 'id', 'delay_time_level',
                     'queue_id',
                     'reconsume_times',
                     # 'store_size',
                     # 'born_timestamp',
                     # 'store_timestamp',
                     'queue_offset',
                     # 'commit_log_offset',
                     # 'prepared_transaction_offset'
                     )
    __ew_members = ('snow_id', 'schedule_rule', 'first_send_timestamp',
                    'latest_send_timestamp', 'valid_at', 'schedule_at',
                    'retry_count', 'priority_level', 'delay_seconds',

                    'task_type', 'producer', 'version',
                    'name', 'job_id', 'payload_type',
                    )

    # __clone_members = ('topic', 'keys', 'tags', 'body', 'schedule_rule', 'first_send_timestamp',
    #                    'valid_at', 'schedule_at', 'task_type', 'name', 'payload_type')

    def _init_message(self, topic_or_message=None, is_received: bool = False):
        from rocketmq.client import Message as RocketMQMessage, ReceivedMessage as RocketMQReceivedMessage

        if isinstance(topic_or_message, str):
            self.topic = topic_or_message
            if is_received:
                self._message = RocketMQReceivedMessage(self.topic)
            else:
                self._message = RocketMQMessage(self.topic)
        else:
            self._message = topic_or_message
            self.from_message(self._message)

        return self._message

    def _validate(self) -> bool:
        if not self.keys:
            raise ValueError('Missing attr: keys')

        if not self.tags:
            raise ValueError('Missing attr: tags')

        return True

    def _to_message(self):
        """
            封装成后端消息中间件接受的消息实例
        :param backend_type:
        :return:
        """
        if not self.topic or not self.body:
            raise ValueError('topic or body must filled')

        message = None

        from rocketmq.client import Message as RocketClientMessage
        message = RocketClientMessage(self.topic)
        if self.keys:
            message.set_keys(self.keys)
        if self.tags:
            message.set_tags(self.tags)
        if self.delay_time_level is not None:
            message.set_delay_time_level(self.delay_time_level)

        message.set_body(self.body)

        ret = {}

        for k in self.__ew_members:
            ret[k] = getattr(self, k, None)

        message.set_property('_ew_property', base64.encodebytes(pickle.dumps(ret, 4)).decode('ascii'))

        ret.clear()
        for k, v in self._msg_property.items():
            ret[k] = v
        message.set_property('_msg_property', base64.encodebytes(pickle.dumps(ret, 4)).decode('ascii'))

        self._message = message

        return self._message

    def _from_message(self, message=None):
        """
            后端消息中间件接受的消息实例转化为 EWMessage
        :return:
        """
        if message is None:
            message = self._message

        if message is None:
            raise ValueError('invalid message')

        try:
            # 解析属性
            for _attr in self.__sys_members:
                setattr(self, _attr, getattr(message, _attr, None))

            _ew_property = base64.decodebytes(self._message.get_property('_ew_property'))
            for k, v in pickle.loads(_ew_property).items():
                setattr(self, k, v)

            _msg_property = base64.decodebytes(self._message.get_property('_msg_property'))
            for k, v in pickle.loads(_msg_property).items():
                self._msg_property[k] = v

        except Exception as e:
            print(traceback.format_exc())
            raise UnknownMessage(str(e))

        return message

    # 获取消息中的自定义属性
    def _get_property(self, prop):
        if not prop:
            raise KeyError()

        return self._msg_property[prop]

    def __str__(self):
        if self._message and self._message._handle is not None:
            return '<EWRocketMQMessage topic={} id={} body={}>'.format(
                str(self.topic or ''),
                str(self.id or ''),
                str(self.body or ''),
            )
        else:
            return '<EWRocketMQMessage topic={} body={}>'.format(
                str(self.topic or ''),
                str(self.body or ''),
            )

    def __repr__(self):
        return self.__str__()
