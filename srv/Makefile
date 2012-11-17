TARG=srv
GOFILES=main.go
DIRS=../lib/conn ../lib/check

PREREQ=$(addsuffix .install, $(DIRS))

all:
	go build

clean: $(addsuffix .clean, $(DIRS))

%.clean:
	$(MAKE) -C $* clean

%.install:
	$(MAKE) -C $* install

#include $(GOROOT)/src/Make.cmd
