"""
    单例
"""


def thread_singleton(cls):
    """
        线程安全的单例
    :param cls:
    :return:
    """
    _instance = {}

    def _singleton(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return _singleton


def process_singleton(cls):
    """
        进程安全的单例
        可以通过标记文件等方式实现
    :param cls:
    :return:
    """
    raise NotImplementedError()


def cluster_singleton(cls):
    """
        集群安全的单例
        可以通过全局共享标记等方式实现
    :param cls:
    :return:
    """
    raise NotImplementedError()
