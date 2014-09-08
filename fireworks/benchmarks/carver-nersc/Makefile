default: help
help:
	@printf "Commands:\n"
	@printf "  qsub - Submit jobs to queue\n"
	@printf "  jobs - Make new set of jobs\n"
	@printf "  clean - Remove jobs and run output files\n"
	@printf "  dbclean - Remove all records from db (permanently!!)\n"
qsub:
	for f in carver-??.pbs; do qsub $$f; done
jobs:
	python make_runs.py
clean:
	@./cleanout
dbclean:
	./sqlcmd "delete from runs"
