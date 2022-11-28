"""
    消费者生成
"""
from echowall.app.conf import EWConfig
from echowall.app.consts import EWMessageBackendTypeEnum


def gen_consumer(group_name, conf: EWConfig, *args, **kwargs):
    consumer = None
    if conf.backend_type == EWMessageBackendTypeEnum.RocketMQ:
        from echowall.rocket_mq.consumer import EWRocketMQConsumer
        consumer = EWRocketMQConsumer(group_name, conf)
    else:
        raise ValueError('unsupported backend type %s' % conf.backend_type.value)

    return consumer
