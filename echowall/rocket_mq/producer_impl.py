"""
    rocketmq 生产者自定义
"""
from rocketmq.client import Producer as MQProducer, _to_bytes
from rocketmq.exceptions import (
    ffi_check, )
from rocketmq.ffi import (
    dll
)


class CustomRocketMQProducer(MQProducer):
    def set_log_path(self, log_path: str):
        """
            设置日志文件路径（暂时不可用）
            默认路径 {user-name}/logs/rocketmq-cpp/
        :param log_path:
        :return:
        """
        ffi_check(dll.SetProducerLogPath(self._handle, _to_bytes(log_path)))

    def set_log_file_num_and_size(self, file_num: int, file_max_size: int):
        """
            配置生产者日志的文件数量和单个文件大小限制（暂时不可用）
        :param file_num:单个日志路径中，最大的日志文件数量
        :param file_max_size:单个日志路径中，单个日志文件最大大小
        :return:
        """
        ffi_check(dll.SetProducerLogFileNumAndSize(self._handle, file_num, file_max_size))

    def set_log_level(self, log_level: int = 3):
        """
            设置日志等级（暂时不可用）
            FATAL = 1
            ERROR = 2
            WARN = 3
            INFO = 4
            DEBUG = 5
            TRACE = 6
            LEVEL_NUM = 7
        :param log_level:
        :return:
        """
        ffi_check(dll.SetProducerLogLevel(self._handle, log_level))
