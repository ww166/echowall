"""
    全局常量
"""
import enum


class EWMessageBackendTypeEnum(enum.Enum):
    """
        消息队列中间件类型
    """
    RocketMQ = 'RocketMQ'
    Redis = 'Redis'


class EWMessageBodyTypeEnum(enum.Enum):
    """
        消息体序列化格式
    """
    PythonObj = 'PythonObj'
    JSON = 'JSON'
    PythonPickle = 'PythonPickle'
    Binary = 'Binary'
    Unknown = 'Unknown'


class EWMessageConsumerModelEnum(enum.IntEnum):
    """
        消息消费模式
    """
    # 广播模式（同一个ConsumerGroup，每个Consumer都会消费一次）
    BROADCASTING = 0
    # 集群模式（同一个ConsumerGroup，仅有一个Consumer消费一次）
    CLUSTERING = 1


class EWSendStatus(enum.IntEnum):
    """
        消息发送结果
    """
    OK = 0
    FLUSH_DISK_TIMEOUT = 1
    FLUSH_SLAVE_TIMEOUT = 2
    SLAVE_NOT_AVAILABLE = 3


class EWConsumeStatus(enum.IntEnum):
    """
        消费状态
    """
    CONSUME_SUCCESS = 0
    RECONSUME_LATER = 1
