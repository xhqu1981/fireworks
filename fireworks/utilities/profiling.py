"""
Simple application-level "profiling" timers.

Sample usage::

    prof = Profiler("Indy500")
    for lap in xrange(200):
        with prof.block("lap"):
            turn_left()
    prof.write()

The output will look like this::

    name,stage,count,time
    Indy500,lap,200,2.176

See documentation of :class:`Profiler` for more detail.
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '2/11/14'

import fnmatch
import os
import sys
import time

#: Environment variable in which to list the enabled profilers
#: Use comma-separated strings, e.g.:
profile_env = "FW_PROFILE"

# Module-level vars

# Dict of profilers parsed from env
_env_profilers = None

# Flag to only write header once
_wrote_header = False


def get_profiler(name):
    """Get profiler for a section of code.
    If the user did not enable profiling for this section, will
    return a NullProfiler. Otherwise will return a Profiler.

    :param name: Name of a given profiler.
    :type name: str
    :return: A profiler instance
    :rtype: Profiler
    """
    # Get enabled profiler patterns from env., if this
    # hasn't already been done.
    if _env_profilers is None:
        _set_env_profilers()

    # See if this profiler is enabled.
    enabled = any(map(lambda pat: fnmatch.fnmatch(name, pat),
                      _env_profilers))

    # Return a real profiler if enabled, else a Null one.
    return Profiler(name) if enabled else NullProfiler()


def _set_env_profilers():
    """Parse enabled profilers from env.
    These are glob-style patterns like "LaunchPad*"
    """
    global _env_profilers
    pstr = os.environ.get(profile_env, "")
    _env_profilers = [s.strip() for s in pstr.split(",")]


class Profiler(object):
    """Simple performance profiler.

    Usage:
        p = Profiler("myname")
        for thing in all_things:
            p.begin("stage1")
            do_stage_1()
            p.end("stage1")
            # alt. 'with' interface
            with p.block("stage2"):
                do_something_else()
        p.write()

    Limitations:
    - Instances are not thread-safe.
    - The set_ns() class method is not thread-safe.
    - The 'with' block() cannot be nested,
      instead use different stages with begin()/end() pairs.
    - The only output format is CSV.
    - There is no (easy) programmatic way to get the results.
    """

    _ns = None

    def __init__(self, name):
        self.name = name
        self._cur_stage = None
        self._stage_times = {}
        self._stage_counts = {}

    @classmethod
    def set_ns(cls, val):
        cls._ns = val

    def block(self, stage):
        self._cur_stage = stage
        return self

    def __enter__(self):
        self.begin(self._cur_stage)

    def __exit__(self, type_, value, tb):
        self.end(self._cur_stage)
        return type_ is None  # not an exception

    def begin(self, stage):
        tm = self._stage_times.get(stage, 0)
        self._stage_times[stage] = tm - time.time()

    def end(self, stage):
        self._stage_times[stage] += time.time()
        count = self._stage_counts.get(stage, 0)
        self._stage_counts[stage] = count + 1

    def _csv(self):
        global _wrote_header
        rows = []
        if not _wrote_header:
            rows.append("name,stage,count,time")
            _wrote_header = True
        ns = "{}.".format(self._ns) if self._ns else ""
        for stage in self._stage_times.iterkeys():
            rows.append("{ns}{n},{s},{c:d},{t:.3f}"
                        .format(ns=ns, n=self.name, s=stage,
                                c=self._stage_counts[stage],
                                t=self._stage_times[stage]))
        return '\n'.join(rows)

    def __str__(self):
        return self._csv()

    def write(self, stream=sys.stdout):
        stream.write(str(self))
        stream.write("\n")


class NullProfiler(Profiler):
    """Support performance profiler interface, but do absolutely nothing.
    This is useful to avoid many tiresome blocks of the form

        # Note: this is NOT necessary with this class!
        if profiling_is_enabled:
            profiler.begin("foo")
        do_foo()
        if_profiling_is_enabled:
            profiler.end("foo")

    By, instead, doing the 'if/else' once:

        profiler = Profiler() if profiling_is_enabled else NullProfiler()
        # Then, forever-more:
        with profiler.block("foo"):
            do_foo()

    """
    def __init__(self):
        Profiler.__init__(self, "")

    def begin(self, stage):
        pass

    def end(self, stage):
        pass

    def write(self, **kwargs):
        pass

    def block(self, stage):
        return self

    def __enter__(self):
        pass

    def __exit__(self, type_, value, tb):
        return type_ is None  # re-raises exception, if there was one
