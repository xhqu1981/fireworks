NERSC Carver scripts
=====================

Scripts to initialize and run Fireworks benchmarks on NERSC Carver.

Use the Makefile as the main driver. Typical sequence of actions:

   1. make clean -- clean up from last run
   2. make jobs -- create clean set of jobs, see make_runs.py
   3. make qsub -- Submit the jobs to the queue

--

Author: Dan Gunter <dkgunter@lbl.gov>
Date: 2014-06-09
