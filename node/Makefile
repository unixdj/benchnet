include $(GOROOT)/src/Make.inc

TARG=node
GOFILES=database.go jobs.go node.go proto.go
#GCFLAGS=-I ${GOPATH}/pkg/linux_amd64
DIRS=../lib/conf ../lib/conn check sched

PREREQ=$(addsuffix .install, $(DIRS))

all:

clean: $(addsuffix .clean, $(DIRS))

%.clean:
	$(MAKE) -C $* clean

%.install:
	$(MAKE) -C $* install

include $(GOROOT)/src/Make.cmd
