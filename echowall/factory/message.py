"""
    消息生成
"""

import typing

from echowall.app.conf import EWConfig
from echowall.app.consts import EWMessageBackendTypeEnum

from echowall.app.message import EWMessage


def gen_message(topic: str, conf: EWConfig, *args, **kwargs) -> typing.Optional[EWMessage]:
    message = None
    if conf.backend_type == EWMessageBackendTypeEnum.RocketMQ:
        from echowall.rocket_mq.message import EWRocketMQMessage
        message = EWRocketMQMessage(topic)
    else:
        raise ValueError('unsupported backend type %s' % conf.backend_type.value)

    return message
