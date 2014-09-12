"""
Delayed (lazy) object instantiation
"""
__author__ = "Dan Gunter <dkgunter@lbl.gov>"
__credits__ = "Bharat Medasani, Anubhav Jain"
__copyright__ = "Copyright 2014, The Materials Project"
__date__ = "11 Sep 2014"

import traceback # debug


def get_visible_attrs(obj):
    """
    Get all 'visible' (read/write) attributes of an obj.
    This excludes attributes starting with a double-underscore
    or those all in upper-case.

    :param obj: Class to examine
    :return: List of attributes (strings)
    """
    attrs = set(dir(obj))
    internal = set(
        filter(lambda s: s.startswith('__') or s.upper() == s, attrs))
    return list(attrs - internal)


class Lazy(object):
    """
    Base class for delayed (lazy) instantiation.

    Instantiate an object and delegate if any of the
    attributes in `watch_attrs` are accessed.
    """

    # Instantiate if any of these attrs are accessed
    watch_attrs = ()

    def __init__(self):
        self.__dict__['__isset'] = set()
        self._set('_obj', None)

    def _instantiate(self, name):
        pass

    def _set(self, name, value):
        self.__dict__[name] = value
        self.__dict__['__isset'].add(name)

    def __getattr__(self, name):
        if self._obj:
            return getattr(self._obj, name)
        if name in self.watch_attrs:
            obj = self._instantiate(name)
            if obj:
                self._set('_obj', obj)
                return getattr(obj, name)
            else:
                return getattr(self, name)  # try again
        elif name in self.__dict__['__isset']:
            return self.__dict__[name]
        else:
            raise AttributeError(self.__class__.__name__ + ":" + name)

    def __setattr__(self, name, value):
        if self._obj:
            setattr(self._obj, name, value)
            return
        if name in self.__dict__['__isset']:
            # first check if attr exists (__setattr__ is called either way)
            self.__dict__[name] = value
        elif name in self.watch_attrs:
            obj = self._instantiate(name)
            if obj:
                setattr(obj, name, value)
                self._set('_obj', obj)
            else:
                setattr(self, name, value)  # try again
        else:
            self.__dict__[name] = value
            #raise AttributeError(self.__class__.__name__ + ":" + name)
