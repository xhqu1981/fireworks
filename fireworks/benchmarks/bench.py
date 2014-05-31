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
import re
import sys
import time
# Third-party
import pymongo
# Local
from fireworks import FireWork, FWorker, LaunchPad, fw_config
from fireworks import FireTaskBase, FWAction, Workflow
from fireworks.core.rocket_launcher import rapidfire
from fireworks.features.multi_launcher import launch_multiprocess

# Set up paths
path = 'fireworks.benchmarks'
if not path in fw_config.USER_PACKAGES:
    fw_config.USER_PACKAGES.append(path)


def print_timings(tm, meta, header=False, strm=None):
    """Print timings in 'tm', along with metadata in 'meta', as CSV.

    Both 'tm' and 'meta' are dictionaries. One row will be written for
    each key in 'tm', with the name set to the key and the timing set to
    the (numeric) value. The key/value pairs from 'meta' will
    be included as additional columns whose values are repeated for each row.

    If 'header' is True, then the CSV header will be printed first.

    Output will go to 'strm' if given, otherwise stdout.
    """
    f = strm or sys.stdout
    w = csv.DictWriter(f, ["name", "tm_sec"] + meta.keys())
    if header:
        w.writeheader()
    d = meta.copy()
    for name, sec in tm.iteritems():
        d.update({"name": name, "tm_sec": sec})
        w.writerow(d)


class BenchmarkTask(FireTaskBase):
    _fw_name = "Benchmark Task"

    def run_task(self, fw_spec):
        return FWAction(stored_data={})


class Benchmark(object):
    task_class = BenchmarkTask
    database = "fireworks_bench"

    # Set up logging
    log = logging.getLogger("benchmark")
    _hnd = logging.StreamHandler()
    _hnd.setFormatter(
        logging.Formatter("(%(name)s) %(levelname)s - %(message)s"))
    log.addHandler(_hnd)

    def __init__(self, vb=0):
        # remember initial dir (FireWorks does chdir)
        self._pdir = os.getcwd()
        if vb < 0 or 'FW_BENCH_QUIET' in os.environ:
            lvl = logging.CRITICAL
        elif vb > 1 or 'FW_BENCH_DEBUG' in os.environ:
            lvl = logging.DEBUG
        elif vb > 0 or 'FW_BENCH_INFO' in os.environ:
            lvl = logging.INFO
        else:
            lvl = logging.WARN
        self.log.setLevel(lvl)
        self.loglevel = logging.getLevelName(lvl)

        try:
            self.lpad = LaunchPad(name=self.database, strm_lvl=self.loglevel)
        except pymongo.errors.ConnectionFailure:
            self.log.error("Failed to connect to MongoDB: is it running?")
            raise

    def reset(self):
        self.lpad.reset('', require_password=False)

    def _reset_dir(self):
        os.chdir(self._pdir)

    def load(self, wf):
        self.log.info("bench.load.start")
        load_t = time.time()
        self.lpad.add_wf(wf)
        load_t = time.time() - load_t
        self.log.info("bench.load.end")
        return load_t

    def run(self, np):
        self.log.info("bench.run.start np={:d}".format(np))
        run_t = time.time()
        if np <= 1:
            rapidfire(self.lpad, FWorker(), strm_lvl=self.loglevel)
        else:
            launch_multiprocess(self.lpad, FWorker(), self.loglevel, 0,
                                np, 0.1, total_node_list=None, ppn=1)
        run_t = time.time() - run_t
        self.log.info("bench.run.end np={:d}".format(np))
        return run_t

    def _rmdir(self, d, pat):
        """Recursively remove all files/dirs matching 'pat' in path 'd'.
        """
        pat_re = re.compile(pat)
        for p in os.listdir(d):
            if not pat_re.match(p):
                continue
            path = os.path  .join(d, p)
            if os.path.isdir(path):
                self._rmdir(path, ".*")
                os.rmdir(path)
            else:
                os.unlink(path)

    def cleanup(self):
        self.log.info("bench.cleanup.start")
        self._rmdir(self._pdir, "launcher_.*")
        self.log.info("bench.cleanup.end")

    def get_workflow(self, n, task_class=None, deps="sequence"):
        """Create a workflow with 'n' tasks.

        :param n: Number of tasks
        :type n: int, >= 1
        :param task_class: Task class or use self.task_class
        :param deps: Type of dependencies
            "sequence" -  No dependencies between tasks.
            "reduce"   -  The last task depends on all previous ones.
            "complex"  -  Dependencies are structured so that the second task
                          depends on the first, and every task after the second
                          depends on the two previous ones.
        :type deps: str
        """
        if n < 1:
            raise ValueError("Bad #tasks: {}".format(n))
        if task_class is None:
            task_class = self.task_class
        tasks = [task_class() for _ in xrange(n)]
        fireworks = [FireWork(tasks[i], fw_id=i) for i in xrange(n)]
        if deps == "sequence":
            _deps = {}
        elif deps == "reduce":
            if n == 1:
                _deps = {}
            else:
                _deps = {n - 1: range(n - 1)}
        elif deps == "complex":
            if n == 1:
                _deps = {}
            elif n == 2:
                _deps = {0: [1]}
            else:
                _deps = {}
                for i in xrange(n - 2):
                    _deps[i] = [i+1, i+2]
                _deps[n - 2] = [n - 1]
        else:
            raise ValueError("Bad value for 'deps': {}".format(deps))
        workflow = Workflow(fireworks, _deps)
        return workflow


def example():
    bench = Benchmark()
    # warmup
    bench.load(bench.get_workflow(1))
    print_timings({}, {'n': 0}, header=True)
    # main
    tm = {}
    for n in (1, 2, 3):
        tm['load'] = bench.load(bench.get_workflow(n))
        tm['run'] = bench.run()
        print_timings(tm, {'n': n})
    bench.cleanup()

if __name__ == '__main__':
    example()