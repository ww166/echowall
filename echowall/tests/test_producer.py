from concurrent.futures import ThreadPoolExecutor
import time
import threading

from echowall import EWMessage, EWClient, EWProducer, EWConfig, EchoWall, EWConsumer, EWConsumeStatus

topic = 'TopicTest'
gid = 'GID_1'
name_srv = 'localhost:9876'


def test_producer_send_sync(producer):
    msg = EWMessage(topic)
    msg.keys = 'send_sync'
    msg.tags = 'XXX'
    msg.body = 'XXXX'
    ret = producer.send_sync(msg)
    assert ret.status == 0


def test_producer_send_sync_multi_thread(producer):
    executor = ThreadPoolExecutor(max_workers=5)
    futures = []
    for _ in range(5):
        futures.append(executor.submit(test_producer_send_sync, producer))

    for future in futures:
        _ret = future.result()


def test_producer_send_oneway(producer):
    msg = EWMessage(topic)
    msg.keys = 'send_oneway'
    msg.tags = 'XXX'
    msg.body = 'XXXX'
    producer.send_oneway(msg)


def test_producer_send_orderly_with_sharding_key(orderly_producer):
    msg = EWMessage(topic)

    msg.keys = 'sharding_message'
    msg.tags = 'sharding'
    msg.body = 'sharding message'
    msg.set_property('property', 'test')

    ret = orderly_producer.send_orderly_with_sharding_key(msg, 'order1')
    assert ret.status == 0


#
#
# def test_transaction_producer(producer):
#     stop_event = threading.Event()
#
#     def on_check(msg):
#         stop_event.set()
#         assert msg.body.decode('utf-8') == msg_body
#         return TransactionStatus.COMMIT
#
#     def on_local_execute(msg, user_args):
#         return TransactionStatus.UNKNOWN
#
#     producer = TransactionMQProducer('transactionTestGroup', on_check)
#     producer.set_name_server_address(name_srv)
#     producer.start()
#
#     msg_body = 'XXXX'
#
#     msg = Message(topic)
#     msg.set_keys('transaction')
#     msg.set_tags('XXX')
#     msg.set_body(msg_body)
#     producer.send_message_in_transaction(msg, on_local_execute)
#     while not stop_event.is_set():
#         time.sleep(2)
#
#     producer.shutdown()


if __name__ == '__main__':
    pass
    # producer = EWProducer(gid)
    # producer.set_name_server_address(name_srv)
    # producer.start()
    #
    # for _ in range(100):
    #     test_producer_send_sync(producer)
    #
    # # test_producer_send_sync_multi_thread(producer)
    # # test_producer_send_oneway(producer)
    # producer.shutdown()
    #
    # # prod = Producer(gid, True)
    # # prod.set_name_server_address(name_srv)
    # # prod.start()
    # # test_producer_send_orderly_with_sharding_key(producer)
    # # prod.shutdown()
    # #
    # # test_transaction_producer()
