#!/usr/bin/env python

import logging
import datetime
from multiprocessing.managers import BaseManager, DictProxy
import string
import sys
import os
import time
import traceback
import socket
import multiprocessing

from fireworks.fw_config import FWData, FW_BLOCK_FORMAT, DS_PASSWORD, \
    FW_LOGGING_FORMAT


__author__ = 'Anubhav Jain, Xiaohui Qu'
__copyright__ = 'Copyright 2012, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Dec 12, 2012'

PREVIOUS_STREAM_LOGGERS = []  # contains the name of loggers that have already been initialized
PREVIOUS_FILE_LOGGERS = []  # contains the name of file loggers that have already been initialized
DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)


def get_fw_logger(name, l_dir=None, file_levels=('DEBUG', 'ERROR'),
                  stream_level='DEBUG', formatter=DEFAULT_FORMATTER,
                  clear_logs=False):
    """
    Convenience method to return a logger.

    :param name: name of the logger that sets the groups, e.g. 'group1.set2'
    :param l_dir: the directory to put the log file
    :param file_levels: iterable describing level(s) to log to file(s). default: ('DEBUG', 'ERROR')
    :param stream_level: level to log to standard output. default: 'DEBUG'
    :param formatter: logging format. default: FW_LOGGING_FORMATTER
    :param clear_logs: whether to clear the logger with the same name
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # anything debug and above passes through to the handler level

    stream_level = stream_level if stream_level else 'CRITICAL'
    # add handlers for the file_levels
    if l_dir:
        for lvl in file_levels:
            f_name = os.path.join(l_dir, name.replace('.', '_') + '-' + lvl.lower() + '.log')
            mode = 'w' if clear_logs else 'a'
            fh = logging.FileHandler(f_name, mode=mode)
            fh.setLevel(getattr(logging, lvl))
            fh.setFormatter(formatter)
            if f_name not in PREVIOUS_FILE_LOGGERS:
                logger.addHandler(fh)
                PREVIOUS_FILE_LOGGERS.append(f_name)

    if (name, stream_level) not in PREVIOUS_STREAM_LOGGERS:
        # add stream handler
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(getattr(logging, stream_level))
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        PREVIOUS_STREAM_LOGGERS.append((name, stream_level))

    return logger


def log_multi(m_logger, msg, log_lvl='info'):
    """
    :param m_logger: (logger) The logger object
    :param msg: (str) a String to log
    :param log_lvl: (str) The level to log at
    """
    _log_fnc = getattr(m_logger, log_lvl.lower())
    if FWData().MULTIPROCESSING:
        _log_fnc("{} : ({})".format(msg, multiprocessing.current_process().name))
    else:
        _log_fnc(msg)


def log_fancy(m_logger, msgs, log_lvl='info', add_traceback=False):
    """
    A wrapper around the logger messages useful for multi-line logs.
    Helps to group log messages by adding a fancy border around it,
    which enhances readability of log lines meant to be read
    as a unit.

    :param m_logger: (logger) The logger object
    :param log_lvl: (str) The level to log at
    :param msgs: ([str]) a String or iterable of Strings
    :param add_traceback: (bool) add traceback text, useful when logging exceptions (default False)
    """

    if isinstance(msgs, basestring):
        msgs = [msgs]

    _log_fnc = getattr(m_logger, log_lvl.lower())

    _log_fnc('----|vvv|----')
    _log_fnc('\n'.join(msgs))
    if add_traceback:
        _log_fnc(traceback.format_exc())
    _log_fnc('----|^^^|----')


def log_exception(m_logger, msgs):
    """
    A shortcut wrapper around log_fancy for exceptions

    :param m_logger: (logger) The logger object
    :param msgs: ([str]) String or iterable of Strings, will be joined by newlines
    """
    return log_fancy(m_logger, msgs, 'error', add_traceback=True)


def create_datestamp_dir(root_dir, l_logger, prefix='block_'):
    """
    Internal method to create a new block or launcher directory. \
    The dir name is based on the time and the FW_BLOCK_FORMAT

    :param root_dir: directory to create the new dir in
    :param l_logger: the logger to use
    :param prefix: the prefix for the new dir, default="block_"
    """

    def get_path():
        time_now = datetime.datetime.utcnow().strftime(FW_BLOCK_FORMAT)
        block_path = prefix + time_now
        return os.path.join(root_dir, block_path)

    full_path = get_path()
    while os.path.exists(full_path):
        import time
        import random
        time.sleep(random.random()/3+0.1)
        full_path = get_path()

    os.mkdir(full_path)

    l_logger.info('Created new dir {}'.format(full_path))
    return full_path


def get_my_ip():
    try:
        return socket.gethostbyname(socket.gethostname())
    except:
        return '127.0.0.1'


def get_my_host():
    return socket.gethostname()


def get_slug(m_str):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    m_str = ''.join(c for c in m_str if c in valid_chars)
    return m_str.replace(' ', '_')


class DataServer(BaseManager):
    """
    Provide a server that can host shared objects between multiprocessing
    Processes (that normally can't share data). For example, a common LaunchPad is
    shared between processes and pinging launches is coordinated to limit DB hits.
    """

    @classmethod
    def setup(cls, launchpad):
        """
        :param launchpad: (LaunchPad) object
        :return:
        """
        DataServer.register('LaunchPad', callable=lambda: launchpad)
        DataServer.register('Running_IDs', callable=lambda: {}, proxytype=DictProxy)
        m = DataServer(address=('127.0.0.1', 0), authkey=DS_PASSWORD)  # random port
        m.start()
        return m


class NestedClassGetter(object):
    """
    Used to help pickle inner classes, e.g. see Workflow.Links
    When called with the containing class as the first argument,
    and the name of the nested class as the second argument,
    returns an instance of the nested class.
    """
    def __call__(self, containing_class, class_name):
        nested_class = getattr(containing_class, class_name)
        # return an instance of a nested_class. Some more intelligence could be
        # applied for class construction if necessary.
        # To support for Pickling of Workflow.Links
        return nested_class()


def explicit_serialize(o):
    o._fw_name = '{{%s.%s}}' % (o.__module__, o.__name__)
    return o
    
class Profiler(object):
    """Simple performance profiler.

    usage:
        p = Profiler()
        for thing in all_things:
            p.set_event(thing)
            p.begin("stage1")
            do_stage_1()
            p.end("stage1")
            # alt. 'with' interface
            with p.block("stage2"):
                do_something_else()
        p.write()

    Note: instances are not thread-safe.
    """
    def __init__(self):
        self.events = {}
        self.key = None
        self.event_keys = []
        self.all_stages = []
        self._cur_stage = None

    def block(self, stage):
        self._cur_stage = stage
        return self

    def __enter__(self):
        self.begin(self._cur_stage)

    def __exit__(self, type_, value, tb):
        self.end(self._cur_stage)
        self._cur_stage = None
        return type_ is None  # re-raises exception, if there was one

    def set_event(self, key):
        self.event_keys.append(key)
        self.key = key
        self.events[key] = {}

    def begin(self, stage):
        ts = time.time()
        self.events[self.key][stage] = [ts, -1]
        self.all_stages.append(stage)

    def end(self, stage):
        ts = time.time()
        self.events[self.key][stage][1] = ts

    def _csv(self):
        stages = ",".join(self.all_stages)
        rows = ["event,{},_total".format(stages)]
        for key in self.event_keys:
            v = self.events[key]
            durations = ["{:.3f}".format(v[s][1] - v[s][0]) for s in self.all_stages]
            total = v[self.all_stages[-1]][1] - v[self.all_stages[0]][0]
            durations.append("{:.3f}".format(total))
            rows.append("{e},{d}".format(e=key, d=",".join(durations)))
        return '\n'.join(rows)

    def __str__(self):
        return self._csv()

    def write(self, stream=sys.stdout):
        stream.write(str(self))
        stream.write("\n")


class NullProfiler(Profiler):
    """Support performance profiler interface, but do absolutely nothing.
    This is useful to avoid many tiresome blocks of the form

        if profiling_is_enabled:
            profiler.begin("foo")
        do_foo()
        if_profiling_is_enabled:
            profiler.end("foo")

    By, instead, doing the 'if/else' once:

        profiler = Profiler() if profiling_is_enabled else NullProfiler()

    Then, simply use the null object wherever you would use the real one.
    """
    def set_event(self, key):
        pass

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
