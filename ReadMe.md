消息中间件配置，消息、消费者、生产者的统一封装,目前仅针对 RocketMQ 进行封装。

### rocket_mq
- 版本 4.9+
- python 3.6

#### 依赖环境
- librocketmq
    - centos 7
    ```
        wget https://github.com/apache/rocketmq-client-cpp/releases/download/2.2.0/rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm
        sudo rpm -ivh rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm
    ```
    - debian
    ```
        wget https://github.com/apache/rocketmq-client-cpp/releases/download/2.2.0/rocketmq-client-cpp-2.2.0.amd64.deb
        sudo dpkg -i rocketmq-client-cpp-2.2.0.amd64.deb
    ```
    - macOS
    ```
        wget https://github.com/apache/rocketmq-client-cpp/releases/download/2.2.0/rocketmq-client-cpp-2.2.0-bin-release-darwin.tar.gz
        tar -xzf rocketmq-client-cpp-2.2.0-bin-release-darwin.tar.gz
        cd rocketmq-client-cpp
        mkdir /usr/local/include/rocketmq
        cp include/* /usr/local/include/rocketmq
        cp lib/* /usr/local/lib
        install_name_tool -id "@rpath/librocketmq.dylib" /usr/local/lib/librocketmq.dylib
    ```
- 详细文档： https://github.com/apache/rocketmq-client-python

#### rocket_mq 服务
- 本地部署
    - 部署文档：https://hub.docker.com/r/xuchengen/rocketmq
    - 注意事项：
    ``` # Linux 或 Mac 剔除 net 启动参数
        docker run -itd \
        --name=rocketmq \
        --hostname rocketmq \
        --restart=always \
        -p 8080:8080 \
        -p 9876:9876 \
        -p 10909:10909 \
        -p 10911:10911 \
        -p 10912:10912 \
        -v rocketmq_data:/home/app/data \
        -v /etc/localtime:/etc/localtime \
        -v /var/run/docker.sock:/var/run/docker.sock \
        xuchengen/rocketmq:latest
    ```
- 常用命令
```
    # 进入docker
    docker exec -it rocketmq /bin/bash
    
    # 配置参考 /home/app/rocketmq/conf/broker.conf
    brokerClusterName = DefaultCluster
    brokerName = broker-a
    brokerId = 0
    deleteWhen = 04
    fileReservedTime = 48
    brokerRole = ASYNC_MASTER
    flushDiskType = ASYNC_FLUSH
    autoCreateTopicEnable = True
    namesrvAddr = 127.0.0.1:9876
    brokerIP1 = 127.0.0.1 # 必填
    
    # 启动命令
    nohup /bin/sh /home/app/rocketmq/bin/mqnamesrv &
    nohup /bin/sh /home/app/rocketmq/bin/mqbroker -c /home/app/rocketmq/conf/broker.conf -n 127.0.0.1:9876 &
    
    # 服务停止
    /bin/sh /home/app/rocketmq/bin/mqshutdown broker
    /bin/sh /home/app/rocketmq/bin/mqshutdown namesrv
```

# 使用方式
1. ### 集成 EchoWall 项目
    1. requirements.txt 添加依赖项： 
        echowall
        或者 直接 
        ```shell
        pip install echowall
        ```
    2. 增加MessageQueue（Rocketmq server 配置）
       ```
       # 消息队列配置信息
       MQ_HOST = 172.18.0.76
       MQ_PORT = 9876
       MQ_TIMEOUT = 6000
       MQ_GROUP_NAME = SDB-GROUP-MAIN    
       ```
    3. flask项目中，配置文件（config.py ）增加对应变量解析
       ```
       #消息队列配置信息
       MQ_HOST = os.getenv("MQ_HOST")
       MQ_PORT = os.getenv("MQ_PORT")
       MQ_TIMEOUT = os.getenv("MQ_TIMEOUT")
       MQ_GROUP_NAME = os.getenv("MQ_GROUP_NAME")`
       ```
    4. flask 项目中， extensions.py 增加 echowall 项目的 初始化变量
        ``` python
        from echowall import (
            EWConfig,
            EWClient
        )
        from base.app_context import auto_app_contexts

        def init_main_mq():
            """
                初始化 EchoWall，启动生产者
            :return:
            """
            from flask import current_app as app
            from apps.commons.message_queue import set_message_record
        
            @auto_app_contexts()
            def _fun(_app):
                conf = EWConfig()
                conf.host = _app.config['MQ_HOST']
                conf.port = int(_app.config['MQ_PORT'])
                conf.timeout = int(_app.config.get('MQ_TIMEOUT', '6000'))
                conf.compress_level = 5
        
                # 生产者发送重试
                conf.send_retry_count = 2
                conf.send_retry_backoff = 1
        
                # 生产者发送消息落库
                conf.record_func = set_message_record
                conf.record_only_success = False
        
                # 消费相关
                conf.consumer_thread_count = 1
        
                rmq_client = EWClient(conf)
        
                group_name = app.config['MQ_GROUP_NAME']
        
                rmq_client.start_producer(group_name)
        
                return rmq_client
        
            return _fun(app)
        ```
    5. 配置topic（参见Topic命名规范）
2. ### 发送消息
    ```python
    try:
        message = mq_client.new_message(MessageTopic.sdb_bu_main.value)
        message.keys = 'send-1'
        message.tags = 'send-1'
        message.body = 'send-%s' % (str(datetime.datetime.now()))
        send_message(message)
    except Exception as e:
        print(traceback.format_exc())
    ```
3. 接收消息
    1. 定义 处理消息的回调函数
        ```python
        import traceback
        
        from echowall import EWConsumeStatus
        
        from apps.extensions import ew
    
        @ew.pc_callback
        def sdb_callback(msg):
            try:
                print(type(msg))
                print(str(msg))
                body = msg.body.decode('utf-8')
                print('Message body is: {}'.format(body))
                print(msg.latest_send_timestamp)
                print(msg.version)
                return EWConsumeStatus.CONSUME_SUCCESS
            except Exception as e:
                print(e)
                print(traceback.format_exc())
                return EWConsumeStatus.RECONSUME_LATER
        ```
    2. 注册回调函数，启动消费者
        ```python
        # start push consumers example
        settings = [
            ('GID_1', 'TopicTest', '*', 'sdb_callback')
        ]
        ew = echo_wall
        rmq_client.bind_echo_wall(ew)
        rmq_client.set_push_consumers_with_echo_wall(settings)
        rmq_client.start_push_consumers()
        while True:
            sleep(3)  
        ```
