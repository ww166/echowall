import threading

from echowall.app.puch_consumer_callback import ConsumerCallback


class EchoWall(object):
    conf = {}
    # push consumer callback name-object map
    _pc_callbacks = {}
    _push_consumer_callback = ConsumerCallback

    def __init__(self):
        self.finalized = False
        self._finalize_mutex = threading.Lock()

    def pc_callback(self, *args, **kwargs):
        def inner_create_pc_callback(*args, **kwargs):
            def _create_pc_callback_cls(func):
                ret = self._pc_callback_from_func(func, *args, **kwargs)
                return ret

            return _create_pc_callback_cls

        if len(args) == 1:
            if callable(args[0]):
                return inner_create_pc_callback(**kwargs)(*args)
            raise TypeError('argument 1 to @task() must be a callable')
        return inner_create_pc_callback(**kwargs)

    def _pc_callback_from_func(self, func, name=None, base=None, **kwargs):
        name = name or func.__qualname__
        full_name = '.'.join([name, func.__module__])
        base = base or self._push_consumer_callback
        if full_name not in self._pc_callbacks:
            run = staticmethod(func)
            pc_callback = type(func.__qualname__, (base,), dict({
                'app': self,
                'name': name,
                'full_name': full_name,
                'run': run,
            }, **kwargs))()
            # for some reason __qualname__ cannot be set in type()
            # so we have to set it here.
            try:
                pc_callback.__qualname__ = func.__qualname__
            except AttributeError:
                pass
            self._pc_callbacks[pc_callback.name] = pc_callback
            pc_callback.bind(self)  # connects task to this app
        else:
            pc_callback = self._pc_callbacks[full_name]
        return pc_callback

    @property
    def pc_callbacks(self):
        return self._pc_callbacks


if __name__ == '__main__':
    echo_wall = EchoWall()


    @echo_wall.pc_callback
    def add(x, y):
        print(x + y)
        return 'Added'


    add.run(1, 2)
    print('123')
