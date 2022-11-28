"""
    rocketmq 自定义消费者
"""
import traceback

from rocketmq.client import PushConsumer as RocketMQConsumer, _to_bytes, ReceivedMessage, ConsumeStatus
from rocketmq.exceptions import (
    ffi_check, )
from rocketmq.ffi import (
    dll
)

from echowall.app.consts import EWConsumeStatus
from echowall.app.exceptions import UnknownMessage

ConsumeStatusMap = {
    EWConsumeStatus.CONSUME_SUCCESS: ConsumeStatus.CONSUME_SUCCESS,
    EWConsumeStatus.RECONSUME_LATER: ConsumeStatus.RECONSUME_LATER
}


class CustomRocketMQConsumer(RocketMQConsumer):
    def subscribe(self, topic, callback, expression='*'):
        """
            消息订阅
        :param topic:
        :param callback:
        :param expression:
        :return:
        """
        from echowall.rocket_mq.message import EWRocketMQMessage

        def _on_message(consumer, msg):
            exc = None
            try:
                consume_result = callback(EWRocketMQMessage(ReceivedMessage(msg), True))
                if not isinstance(consume_result, EWConsumeStatus):
                    raise ValueError('invalid consumer result')
                if consume_result != EWConsumeStatus.CONSUME_SUCCESS:
                    print('Warring: consume failed for message %s' % str(msg))
                return ConsumeStatusMap.get(consume_result)
            except UnknownMessage as e:
                print('UnknownMessage and will ignored', str(msg))
                return ConsumeStatus.CONSUME_SUCCESS
            except BaseException as e:
                print('Exception and will retry', str(traceback.format_exc()))
                exc = e
                return ConsumeStatus.RECONSUME_LATER
            finally:
                if exc:
                    raise exc

        ffi_check(dll.Subscribe(self._handle, _to_bytes(topic), _to_bytes(expression)))
        self._register_callback(_on_message)

    def set_log_path(self, log_path: str):
        """
            设置日志文件路径（暂时不可用）
            默认路径： {user-home}/logs/rocketmq-cpp/
        :param log_path:
        :return:
        """
        ffi_check(dll.SetPushConsumerLogPath(self._handle, _to_bytes(log_path)))

    def set_log_file_num_and_size(self, file_num: int, file_max_size: int):
        """
            配置生产者日志的文件数量和单个文件大小限制（暂时不可用）
        :param file_num:单个日志路径中，最大的日志文件数量
        :param file_max_size:单个日志路径中，单个日志文件最大大小
        :return:
        """
        ffi_check(dll.SetPushConsumerLogFileNumAndSize(self._handle, file_num, file_max_size))

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
        ffi_check(dll.SetPushConsumerLogLevel(self._handle, log_level))
