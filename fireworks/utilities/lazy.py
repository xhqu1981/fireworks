"""
Delayed (lazy) object instantiation
"""
__author__ = "Dan Gunter <dkgunter@lbl.gov>"
__credits__ = "Bharat Medasani, Anubhav Jain"
__copyright__ = "Copyright 2014, The Materials Project"
__date__ = "11 Sep 2014"


class Lazy(object):
    """
    Base class for delayed (lazy) instantiation.

    Instantiate an object and delegate if any of the
    attributes in `watch_attrs` are accessed.
    """

    # Instantiate if any of these attrs are accessed
    watch_attrs = ()

    def __init__(self):
        self._obj = None

    def _instantiate(self, name):
        pass

    def __getattr__(self, name):
        if not self._obj:
            if name in self.watch_attrs:
                self._obj = self._instantiate(name)
            else:
                raise AttributeError(name)
        return getattr(self._obj, name)

    def __setattr__(self, name, value):
        if not self._obj:
            if name in self.watch_attrs:
                self._obj = self._instantiate(name)
            else:
                raise AttributeError(name)  # no monkey-patching
        setattr(self._obj, name, value)
