include $(GOROOT)/src/Make.inc

TARG=node
GOFILES=jobs.go node.go proto.go
DIRS=../lib/conf ../lib/conn check db sched

PREREQ=$(addsuffix .install, $(DIRS))

all:

clean: $(addsuffix .clean, $(DIRS))

%.clean:
	$(MAKE) -C $* clean

%.install:
	$(MAKE) -C $* install

include $(GOROOT)/src/Make.cmd
