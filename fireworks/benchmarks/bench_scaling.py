"""
Simple scaling study.
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '5/30/14'

# System
import argparse
import re
import sys
import threading
# Local
from fireworks.benchmarks.bench import Benchmark, print_timings


def load(bench, tasks, workflows, deps):
    for ntasks in range(*tasks):
        for nwf in range(*workflows):
            # run 'nwf' workflows with 'ntasks' tasks in each
            bench.log.info("load.start nwf={:d} ntasks={:d}"
                           .format(nwf, ntasks))
            tm = 0
            for w in xrange(nwf):
                tm += bench.load(bench.get_workflow(ntasks))
            bench.log.info("load.end")
            print_timings({'load': tm},
                          {'nwf': nwf, 'ntasks': ntasks, 'type': deps})


def run(bench, meta, np):
    bench.log.info("scaling.run.start")
    tm = bench.run(np)
    bench.log.info("scaling.run.end")
    print_timings({'run': tm}, meta)


def irange(v):
    m = re.match("(\d+)(?::(\d+):(\d+))?", v)
    if m is None:
        raise ValueError("Bad range min:max:step in '{}'".format(v))
    g = m.groups()
    if g[1] is None:
        return int(g[0]), int(g[0]) + 1, 1
    else:
        return int(g[0]), int(g[1]) + 1, int(g[2])


def main():
    ap = argparse.ArgumentParser("Load or run workflows")
    ap.add_argument("--mode", dest="mode", default="load",
                    help="Mode: load, run")
    ap.add_argument("--tasks", dest="tasks", type=irange, default='1',
                    help="Number of tasks")
    ap.add_argument("--type", dest="type", default="sequence",
                    help="Workflow type (sequence, reduce, complex)")
    ap.add_argument("--workflows", dest="workflows", type=irange, default='1',
                    help="Number of workflows")
    ap.add_argument("--reset", dest="reset", action="store_true",
                    help="Reset the FireWorks DB before starting")
    ap.add_argument("--np", dest="np", type=int, default=1,
                    help="Parallelism, for 'run' mode only")
    ap.add_argument("-v", "--verbose", dest="vb", action="count", default=0,
                    help="Increase log level to INFO, then DEBUG")
    ap.add_argument("-q", "--quiet", dest="quiet", action="store_true",
                    help="Turn off log messages")
    args = ap.parse_args()
    vb = -1 if args.quiet else min(args.vb, 2)

    bench = Benchmark(vb=vb)
    if args.reset:
        bench.reset()

    print_timings({}, {'nwf': 0, 'ntasks': 0, 'type': ''}, header=True)

    if args.mode == "load":
        load(bench, args.tasks, args.workflows, args.type)
    elif args.mode == "run":
        max_wf, max_tasks = args.workflows[1] - 1, args.tasks[1] - 1
        meta = {'nwf': max_wf, 'ntasks': max_tasks, 'type': args.type}
        run(bench, meta, args.np)
    else:
        ap.error("Bad mode '{}' not load or run".format(args.mode))

    bench.log.info("scaling.cleanup.start")
    bench.cleanup()
    bench.log.info("scaling.cleanup.end")

    return 0



if __name__ == '__main__':
    sys.exit(main())
