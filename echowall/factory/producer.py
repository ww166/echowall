"""
    生产者生成
"""
from echowall.app.conf import EWConfig
from echowall.app.consts import EWMessageBackendTypeEnum


def gen_producer(group_name: str, conf: EWConfig, *args, **kwargs):
    producer = None
    if conf.backend_type == EWMessageBackendTypeEnum.RocketMQ:
        from echowall.rocket_mq.producer import EWRocketMQProducer
        producer = EWRocketMQProducer(group_name, conf)
    else:
        raise ValueError('unsupported backend type %s' % conf.backend_type.value)

    return producer
