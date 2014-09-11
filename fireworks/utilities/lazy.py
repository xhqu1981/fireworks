"""
Delayed (lazy) object instantiation
"""
__author__ = "Dan Gunter <dkgunter@lbl.gov>"
__credits__ = "Bharat Medasani, Anubhav Jain"
__copyright__ = "Copyright 2014, The Materials Project"
__date__ = "11 Sep 2014"


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
        self._set('_obj', None)

    def _instantiate(self, name):
        pass

    def _set(self, name, value):
        self.__dict__[name] = value

    def __getattr__(self, name):
        if not self._obj:
            if name in self.watch_attrs:
                obj = self._instantiate(name)
                if obj:
                    self._set('_obj', obj)
                else:
                    return getattr(self, name)  # try again
            else:
                raise AttributeError(name)
        return getattr(self._obj, name)

    def __setattr__(self, name, value):
        if not self._obj:
            if name in self.watch_attrs:
                obj = self._instantiate(name)
                if obj:
                    self._set('_obj', obj)
                else:
                    setattr(self, name, value)  # try again
            else:
                raise AttributeError(name)  # no monkey-patching
        setattr(self._obj, name, value)

    def _unwatch(self, name):
        obj = self._instantiate(name)
        if obj:
            self._set('_obj', obj)
        else:
            return getattr(self, name)  # try again


def main():

    class Real(object):
        def __init__(self):
            print("init Real")
            self.bar = 2
            self.foo = 3

    class L(Lazy):
        watch_attrs = ('foo', 'bar')

        def _instantiate(self, name):
            print("attribute {}".format(name))
            if name == 'foo':
                self._set('foo', 1)
                return None
            else:
                return Real()

    lz = L()
    print("foo = {}".format(lz.foo))
    print("bar = {}".format(lz.bar))
    print("bar = {}".format(lz.bar))
    print("foo = {}".format(lz.foo))

if __name__ == '__main__':
    main()