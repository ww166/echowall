"""
    PF 通用消息结构
"""
import copy
import datetime
import enum
import json
import time
import typing

from snowflake import SnowflakeGenerator

from echowall.app.consts import EWMessageBackendTypeEnum


class EWMessage:
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

    _message = None
    _message_backend_type: EWMessageBackendTypeEnum = None

    _msg_property: typing.Dict[str, str] = {}

    topic: str = None
    keys: str = None
    tags: str = None
    body: str = None
    delay_time_level: int = None
    id: str = None
    queue_id: str = None
    reconsume_times: int = None
    # store_size: int = None
    # born_timestamp: int = None
    # store_timestamp: int = None

    queue_offset: int = None
    # commit_log_offset: int = None
    # prepared_transaction_offset: int = None

    """
        消息元数据
    """
    snow_id: str = str(next(SnowflakeGenerator(64)))  # 消息编号，采用雪花算法
    # schedule_rule: str = ''  # 可以采用corn 定时任务写法，解析后，排序追加到上面的参数。

    first_send_timestamp: int = int(time.time())  # 首次发送时间（producer 发送消息时间）
    latest_send_timestamp: int = int(time.time())  # 末次发送时间（重试时 的发送消息时间）
    valid_at: datetime.datetime = None  # 消息的有效截止时间（超过截止时间，有业务层自行处理）
    # schedule_at: typing.List[datetime.datetime] = None  # 消息的有效截止时间（超过截止时间，有业务层自行处理）

    retry_count: int = 0  # 默认为0， 重试时 +1
    priority_level: int = -1  # 消息优先级： -1 未设置
    delay_seconds: int = -1  # 消息延时秒数： -1 未设置

    """
        消息内容元数据
    """
    task_type: str = ''  # 任务的处理类型： dbSchema, dbBatchData, dbFlowData, dbFullData, systemMsg, appMsg 等
    producer: str = ''  # 生产者
    version: str = '1.0.0'  # 消息体版本
    name: str = ''  # 消息名称
    job_id: str = ''  # 任务ID
    body_type: str = 'python-obj'  # payload 结构类型： python-obj, json string , pickle raw data, None 等

    def __init__(self,
                 topic_or_message=None,
                 is_received: bool = False):
        super(EWMessage, self).__init__()

        if isinstance(topic_or_message, enum.Enum):
            topic_or_message = topic_or_message.value

        self._init_message(topic_or_message, is_received)

    def _init_message(self, topic: str = None, is_received: bool = False):
        raise NotImplementedError()

    def validate(self) -> bool:
        if not self.topic:
            raise ValueError('Missing attr: topic')

        if not self.body:
            raise ValueError('Missing attr: body')

        return self._validate()

    def to_message(self):
        """
            封装成后端消息中间件接受的消息实例
        :return:
        """
        return self._to_message()

    def to_json(self):
        """
            将消息内容转化为 json 字符串
        :return:
        """
        return json.dumps(
            dict([(k, getattr(self, k)) for k in dir(self) if not str(k).startswith('_') and not callable(getattr(self, k))])
        )

    def from_message(self, message=None):
        """
            后端消息中间件接受的消息实例转化为 EWMessage
        :return:
        """
        return self._from_message(message)

    def clone(self):
        message = copy.deepcopy(self)
        message.id = None
        message.queue_id = None
        message.reconsume_times = 0
        message.snow_id = str(next(SnowflakeGenerator(64)))
        message.first_send_timestamp = int(time.time())
        message.latest_send_timestamp = message.first_send_timestamp
        message.retry_count = 0

        return self._clone(message)

    def set_property(self, key: str, value: str):
        """
            设置自定义消息属性
        :param key:
        :param value:
        :return:
        """
        return self._set_property(key, value)

    # 获取消息中的自定义属性
    def get_property(self, prop):
        return self._get_property(prop)

    def __str__(self):
        if self.body:
            return self.body
        else:
            return super(EWMessage, self).__str__()

    def __bytes__(self):
        if self.body:
            return self.body

        return b''

    def __repr__(self):
        return '<ReceivedMessage topic={} body={}>'.format(
            repr(self.topic or ''),
            repr(self.body or ''),
        )

    def _validate(self) -> bool:
        return True

    def _to_message(self):
        """
            封装成后端消息中间件接受的消息实例
        :return:
        """
        raise NotImplementedError()

    def _from_message(self, message=None):
        """
            后端消息中间件接受的消息实例转化为 EWMessage
        :return:
        """
        raise NotImplementedError()

    def _clone(self, message=None):
        return message

    def _set_property(self, key: str, value: str):
        """
            设置自定义消息属性
        :param key:
        :param value:
        :return:
        """
        if key in dir(self):
            raise KeyError('this key %s is system reserved')

        self._msg_property[key] = value

    def _get_property(self, prop):
        if not prop:
            raise KeyError()

        return self._msg_property[prop]
