include $(GOROOT)/src/Make.inc

TARG=srv
GOFILES=main.go
DIRS=../lib/conn

PREREQ=$(addsuffix .install, $(DIRS))

all:

clean: $(addsuffix .clean, $(DIRS))

%.clean:
	$(MAKE) -C $* clean

%.install:
	$(MAKE) -C $* install

include $(GOROOT)/src/Make.cmd
