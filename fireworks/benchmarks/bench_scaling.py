"""
Simple scaling study.
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '5/30/14'

# System
import argparse
import logging
import os
import socket
import sqlite3
import sys
import time
# Local
from fireworks.benchmarks.bench import Benchmark
from fireworks.utilities import timing

_client = None
nodes = 1
_id = None

_log = logging.getLogger("benchmark")


def get_client():
    global _client
    if _client is None:
        _client = socket.getfqdn()
        if 'PBS_JOBID' in os.environ:
            _client += "-" + os.environ['PBS_JOBID']
    return _client


def init_results(filename):
    """Create and return result DB connection.
    """
    db = sqlite3.connect(filename)
    db.execute("""CREATE TABLE IF NOT EXISTS
               runs (mode char(4), runid char(32),
               tasks integer,
               wftasks integer, clients integer, wftype char(7),
               client char(32),
               start double, end double, dur double)""")
    db.commit()
    return db


def insert_result(db, action, tasks, workflows, deps, t0, t1):
    """Insert one result into DB.
    """
    cli = get_client()
    db.execute("insert into runs values ('{}', '{}', {:d}, {:d}, {:d}, '{}',"
               "'{}', {:.6f}, {:.6f}, {:.6f})"
               .format(action, _id, tasks * workflows, tasks, nodes, deps, cli, 
	               t0, t1, t1 - t0))
    db.commit()


def load(bench, tasks, workflows, deps, db):
    bench.log.info("scaling.load.start tasks={:d} workflows={:d}"
                   .format(tasks, workflows))
    t0 = time.time()
    # load 'workflows' workflows with 'tasks' tasks in each
    for _ in xrange(workflows):
        bench.load(bench.get_workflow(tasks))
    t1 = time.time()
    insert_result(db, 'load', tasks, workflows, deps, t0, t1)
    bench.log.info("scaling.load.end")


def run(bench, tasks, workflows, deps, np, db):
    bench.log.info("scaling.run.start")
    t0 = time.time()
    t1 = t0 + bench.run(np)
    insert_result(db, 'run', tasks, workflows, deps, t0, t1)
    bench.log.info("scaling.run.end")


def main():
    global nodes, _id

    # Parse command-line.
    ap = argparse.ArgumentParser("Load or run workflows")
    ap.add_argument("--mode", dest="mode", default="load",
                    help="Mode: load, run")
    ap.add_argument("--tasks", dest="tasks", type=int, default='1',
                    help="Number of tasks per workflow")
    ap.add_argument("--type", dest="type", default="sequence",
                    help="Workflow type (sequence, reduce, complex)")
    ap.add_argument("--workflows", dest="workflows", type=int, default='1',
                    help="Number of workflows")
    ap.add_argument("--reset", dest="reset", action="store_true",
                    help="Reset the FireWorks DB before starting")
    ap.add_argument("--np", dest="np", type=int, default=1,
                    help="Parallelism, for 'run' mode only")
    ap.add_argument("--clients", dest="clients", type=int, default=1)
    ap.add_argument("--run", dest="rnum", type=int, default=1, help="Run number (1)")
    ap.add_argument("--id", dest="ident", default="", help="Run id ('')")
    ap.add_argument("--rfile", dest="rfile", default="/tmp/benchmarks.sqlite",
                    help="Results SQLite file (%(default)s)")
    ap.add_argument("-v", "--verbose", dest="vb", action="count", default=0,
                    help="Increase log level to INFO, then DEBUG")
    ap.add_argument("-t", "--trace", dest="trace", action="store_true",
                    help="Turn on tracing output to stderr")
    ap.add_argument("-q", "--quiet", dest="quiet", action="store_true",
                    help="Turn off log messages")
    args = ap.parse_args()

    # Set verbosity (will be passed to Benchmark class)
    vb = -1 if args.quiet else min(args.vb, 2)
    _log.setLevel((logging.FATAL, logging.WARN, logging.INFO, logging.DEBUG)
                  [vb + 1])

    # Turn on tracing if requested
    if args.trace:
        timing.Timer.set_trace(True, stream=sys.stderr)

    # Set working directory.
    working_dir = os.path.join(os.getcwd(), get_client())
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)
    bench = Benchmark(vb=vb, basedir=working_dir)
    if args.reset:
        bench.reset()

    # Create results database.
    # - adjust results db-file to working dir, if relative
    if not os.path.isabs(args.rfile):
        results_file = os.path.join(working_dir, args.rfile)
    else:
        results_file = args.rfile
    # - initialize sqlite DB to use the file
    db = init_results(results_file)

    # Set some global vars (gasp!)
    _id = args.ident + "{:d}".format(args.rnum)
    nodes = args.np * args.clients

    # Run the load or run command.
    if args.mode == "load":
        load(bench, args.tasks, args.workflows, args.type, db)
    elif args.mode == "run":
        run(bench, args.tasks, args.workflows, args.type, args.np, db)
    else:
        ap.error("Bad mode '{}' not load or run".format(args.mode))

    # Cleanup.
    bench.log.info("scaling.cleanup.start")
    try:
        bench.cleanup()
    except:
        pass
    bench.log.info("scaling.cleanup.end")

    # Print out timers if any
    if timing.any_fw_timers():
        print("Timers\n-------")
        timing.print_fw_timers()

    return 0


if __name__ == '__main__':
    sys.exit(main())
