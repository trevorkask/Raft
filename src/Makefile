RACE ?= -race
RUN ?=

.PHONY: all raft1-build raft1

all: raft1

raft1-build:
	go build $(RACE) -o main/raft1d main/raft1d.go

raft1: raft1-build
	cd raft1 && go test -v $(RACE) $(RUN)
