import collections

#def names(iterable):
#    return ( i.name for i in iterable )

def is_non_str_iterable(c):
    return isinstance(c, collections.Iterable) and not isinstance(c, str)
