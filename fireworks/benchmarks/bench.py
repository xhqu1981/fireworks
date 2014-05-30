"""
Common code for Fireworks benchmarks.

See example() for example usage.

Will log to stderr at default level of WARN. To increase logging level,
set env var FW_BENCH_{INFO,DEBUG}. To turn off all logging, set FW_BENCH_QUIET.
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '5/30/14'

# System
import csv
import logging
import os
import sys
import time
# Third-party
import pymongo
# Local
from fireworks import FireWork, FWorker, LaunchPad, fw_config
from fireworks import FireTaskBase, FWAction, Workflow
from fireworks.core.rocket_launcher import rapidfire


def print_timings(tm, meta, header=False):
    w = csv.DictWriter(sys.stdout, ["name", "tm_sec"] + meta.keys())
    if header:
        w.writeheader()
    d = meta.copy()
    for name, sec in tm.iteritems():
        d.update({"name": name, "tm_sec": sec})
        w.writerow(d)


class EmptyTask(FireTaskBase):
    _fw_name = "Empty Task"

    def run_task(self, fw_spec):
        return FWAction(stored_data={})


class Benchmark(object):
    task_class = EmptyTask

    def __init__(self):

        # Set up logging
        _log = logging.getLogger("benchmark")
        _hnd = logging.StreamHandler()
        _hnd.setFormatter(
            logging.Formatter("(%(name)s) %(levelname)s - %(message)s"))
        _log.addHandler(_hnd)
        if 'FW_BENCH_QUIET' in os.environ:
            lvl = logging.CRITICAL
        elif 'FW_BENCH_DEBUG' in os.environ:
            lvl = logging.DEBUG
        elif 'FW_BENCH_INFO' in os.environ:
            lvl = logging.INFO
        else:
            lvl = logging.WARN
        _log.setLevel(lvl)
        self.loglevel, self.log = logging.getLevelName(lvl), _log

        # Set up paths
        fw_config.USER_PACKAGES.append('fireworks.benchmarks')

        try:
            self.lpad = LaunchPad(name='fireworks_test', strm_lvl='ERROR')
        except pymongo.errors.ConnectionFailure:
            self.log.error("Failed to connect to MongoDB: is it running?")
            raise
        self.lpad.reset('', require_password=False)

    def run_local(self, wf):
        self.log.info("run_local.start")
        add_t = time.time()
        self.lpad.add_wf(wf)
        add_t = time.time() - add_t
        run_t = time.time()
        rapidfire(self.lpad, FWorker(), strm_lvl=self.loglevel)
        run_t = time.time() - run_t
        self.log.info("run_local.end")
        return dict(add=add_t, run=run_t)

    def get_workflow(self, n, task_class=None):
        """Create a workflow with 'n' tasks.

        Dependencies are structured so that the second task
        depends on the first, and every task after the second
        depends on the two previous ones.

        :param n: Number of tasks
        :type n: int
        """
        if task_class is None:
            task_class = self.task_class
        tasks = [task_class() for _ in xrange(n)]
        fireworks = [FireWork(tasks[i], fw_id=i) for i in xrange(n)]
        if n == 1:
            deps = {}
        elif n == 2:
            deps = {0: [1]}
        else:
            deps = {}
            for i in xrange(n - 2):
                deps[i] = [i+1, i+2]
            deps[n - 2] = [n - 1]
        workflow = Workflow(fireworks, deps)
        return workflow


def example():
    bench = Benchmark()
    first = True
    for n in (1, 2, 3):
        tm = bench.run_local(bench.get_workflow(n))
        print_timings(tm, {'n': n}, header=first)
        first = False

if __name__ == '__main__':
    example()