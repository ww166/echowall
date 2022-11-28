"""
    配置结构体
"""
import typing

from echowall.app.consts import EWMessageBackendTypeEnum
from echowall.app.consts import EWMessageConsumerModelEnum


class EWConfig:
    """
        EchoWall消息中间件通用配置
    """
    # 消息中间件类型
    backend_type: EWMessageBackendTypeEnum = EWMessageBackendTypeEnum.RocketMQ

    # 生产者，消费者 共用的 服务器配置
    host: str = None
    port: int = None

    # 生产者 发送消息配置： 是否发送消息时进行排序
    orderly: bool = False

    # 生产者 超时（单位 毫秒）
    timeout: int = 3000
    # 生产者消息压缩： 0（最快）~9（最小）
    compress_level: int = 5
    # 生产者 限制消息最大值 （单位 B）
    max_message_size: int = 1024 * 128

    # 生产端 发送重试配置（Rocketmq 自带重试功能，可以不配置）
    send_retry_count: int = None
    send_retry_backoff: typing.Union[typing.Callable, int] = None

    # 消费者 模式
    consumer_mode: EWMessageConsumerModelEnum = EWMessageConsumerModelEnum.CLUSTERING
    # 消费者 线程限制（Rocketmq 中的配置）
    consumer_thread_count: int = None
    # 消费者 实例名称
    instance_name: str = None

    # 认证相关（Rocketmq）
    access_key: str = None
    access_secret: str = None
    channel: str = None

    # 落库相关
    record_only_success: bool = True
    record_func: typing.Callable = None

    # 日志相关
    producer_log_path: str = None
    producer_log_file_num: int = None
    producer_log_file_size: int = None
    producer_log_level: int = 3

    consumer_log_path: str = None
    consumer_log_file_num: int = None
    consumer_log_file_size: int = None
    consumer_log_level: int = None
