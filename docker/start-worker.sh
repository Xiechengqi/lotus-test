#/bin/bash

rm -rf $HOME/.lotusworker/data-transfer
rm -rf $HOME/.lotusworker/heapprof
rm -rf $HOME/.lotusworker/journal
rm -rf $HOME/.lotusworker/kvlog
rm -rf $HOME/.lotusworker/repo.lock

lotus-worker run --precommit1=false --precommit2=false --commit=true