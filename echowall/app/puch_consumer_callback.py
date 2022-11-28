"""
    消费者回调
"""


class ConsumerCallback(object):
    from_config = (())

    @classmethod
    def bind(cls, app):
        cls._app = app
        conf = app.conf
        # for attr_name, config_name in cls.from_config:
        #     if getattr(cls, attr_name, None) is None:
        #         setattr(cls, attr_name, conf[config_name])
        return app
