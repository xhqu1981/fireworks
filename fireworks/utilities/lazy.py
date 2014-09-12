"""
Delayed (lazy) object instantiation
"""
__author__ = "Dan Gunter <dkgunter@lbl.gov>"
__credits__ = "Bharat Medasani, Anubhav Jain"
__copyright__ = "Copyright 2014, The Materials Project"
__date__ = "11 Sep 2014"

import traceback # debug


def get_external_attrs(cls):
    """
    Get all 'external' (read/write) attributes of a class.
    This excludes attributes starting with a double-underscore
    or those all in upper-case.

    :param cls: Class to examine
    :return: List of attributes (strings)
    """
    attrs = set(dir(cls))
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
        #traceback.print_stack()
        self.__dict__['__isset'] = set()
        self._set('_obj', None)

    def _instantiate(self, name):
        pass

    def _set(self, name, value):
        #print("@@ SET {} on {}".format(name, self.__class__.__name__))
        self.__dict__[name] = value
        self.__dict__['__isset'].add(name)

    def __getattr__(self, name):
        #print("@@ getattr {} on {}".format(name, self.__class__.__name__))
        if self._obj:
            #print("@@ {}: in obj".format(name))
            return getattr(self._obj, name)
        #print("@@ {}: NO obj".format(name))
        if name in self.watch_attrs:
            obj = self._instantiate(name)
            if obj:
                self._set('_obj', obj)
                return getattr(obj, name)
            else:
                return getattr(self, name)  # try again
        elif name in self.__dict__['__isset']:
            #print("@@ -- already set")
            return self.__dict__[name]
        else:
            raise AttributeError(self.__class__.__name__ + ":" + name)

    def __setattr__(self, name, value):
        if self._obj:
            setattr(self._obj, name, value)
            return
        if name in self.watch_attrs:
            #print("@@ -- watched")
            obj = self._instantiate(name)
            if obj:
                setattr(obj, name, value)
                self._set('_obj', obj)
            else:
                setattr(self, name, value)  # try again
        elif name in self.__dict__['__isset']:
            #print("@@ -- already set")
            self.__dict__[name] = value
        else:
            raise AttributeError(self.__class__.__name__ + ":" + name)
