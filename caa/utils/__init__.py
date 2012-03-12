import collections

# from django.utils.functional
empty = object()
def new_method_proxy(func):
    def inner(self, *args):
        if self._wrapped is empty:
            self._setup()
        return func(self._wrapped, *args)
    return inner


class LazyObject(object):
    """
    A wrapper for another class that can be used to delay instantiation of the
    wrapped class.

    By subclassing, you have the opportunity to intercept and alter the
    instantiation. If you don't need to do that, use SimpleLazyObject.
    """
    def __init__(self):
        self._wrapped = empty

    __getattr__ = new_method_proxy(getattr)

    def __setattr__(self, name, value):
        if name == "_wrapped":
            # Assign to __dict__ to avoid infinite __setattr__ loops.
            self.__dict__["_wrapped"] = value
        else:
            if self._wrapped is empty:
                self._setup()
            setattr(self._wrapped, name, value)

    def __delattr__(self, name):
        if name == "_wrapped":
            raise TypeError("can't delete _wrapped.")
        if self._wrapped is empty:
            self._setup()
        delattr(self._wrapped, name)

    def _setup(self):
        """
        Must be implemented by subclasses to initialise the wrapped object.
        """
        raise NotImplementedError

    def reset(self):
        """ 
        Resets the wrapped instance. It forces _setup to be run
        next time data is accessed. 
        """
        self._wrapped = empty

    # introspection support:
    __members__ = property(lambda self: self.__dir__())
    __dir__ = new_method_proxy(dir)

#######################################################

def names(iterable):
    return ( i.name for i in iterable )

def is_non_str_iterable(c):
    return isinstance(c, collections.Iterable) and not isinstance(c, str)
