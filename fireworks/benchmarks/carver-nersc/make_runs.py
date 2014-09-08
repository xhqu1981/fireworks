#!/usr/bin/env python

processors = 8

def main():
    buf = open("carver.pbs.template").read()
    i = 1
    #for clients in (1, 16, 64):
    #    for ntasks in (1000, 2000, 3000, 4000, 5000):
    wftype = "complex"
    # taskrange = [50, 100, 200]
    taskrange = [500, 1000]
    clientrange = [1, 8]
    for clients in clientrange:
        for ntasks in taskrange:
            for wf in range(3):
                if wf == 0:
                    workflows = 1
                elif wf == 1:
                    workflows = min(taskrange)
                    if workflows == ntasks:
                        continue # duplicates next case
                else:
                    workflows = ntasks
                tasks = ntasks / workflows
                if clients > processors:
                    nodes = clients / processors
                    ppn = processors
                else:
                    nodes = 1
                    ppn = clients
                f = open("carver-{:02d}.pbs".format(i), "w")
                f.write(buf.format(**locals()))
                i += 1

if __name__ == '__main__':
    main()
