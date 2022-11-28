import re


def is_ip(v: str) -> bool:
    """
        判断是否为IP地址
        暂时只处理IP4
    :param v:
    :return:
    """
    _regex = "^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$"

    return bool(re.fullmatch(_regex, v))
