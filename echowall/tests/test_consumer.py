import time
import threading

from echowall import EWMessage, EWClient, EWProducer, EWConfig, EchoWall, EWConsumer, EWConsumeStatus

# from rocket_mq.exceptions import PushConsumerStartFailed

topic = 'TopicTest'
gid = 'GID_1'
name_srv = 'localhost:9876'


def test_push_consumer(push_consumer):
    stop_event = threading.Event()
    errors = []

    def on_message(msg):
        stop_event.set()
        try:
            print(msg.body.decode('utf-8'))
            return EWConsumeStatus.CONSUME_SUCCESS
        except Exception as exc:
            errors.append(exc)
            return EWConsumeStatus.RECONSUME_LATER

    push_consumer.subscribe(topic, on_message)
    push_consumer.start()
    while not stop_event.is_set():
        time.sleep(2)
    if errors:
        raise errors[0]


def test_push_consumer_reconsume_later(push_consumer):
    stop_event = threading.Event()
    raised_exc = threading.Event()
    errors = []

    def on_message(msg):
        if not raised_exc.is_set():
            raised_exc.set()
            # 消费失败后，默认延迟10秒后重新消费，默认共16次
            return EWConsumeStatus.RECONSUME_LATER

        stop_event.set()
        try:
            print(msg.body.decode('utf-8'))
        except Exception as exc:
            errors.append(exc)
            return EWConsumeStatus.CONSUME_SUCCESS

    push_consumer.subscribe(topic, on_message)
    push_consumer.start()
    while not stop_event.is_set():
        time.sleep(2)
    if errors:
        raise errors[0]


if __name__ == '__main__':
    pass
    # consumer = EWProducer(gid)
    # consumer.set_name_server_address(name_srv)
    #
    # test_push_consumer(consumer)
    # test_push_consumer_reconsume_later(consumer)
    #
    # consumer.shutdown()
