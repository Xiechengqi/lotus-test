#/bin/bash

rm -rf $HOME/.lotus/data-transfer
rm -rf $HOME/.lotus/heapprof
rm -rf $HOME/.lotus/journal
rm -rf $HOME/.lotus/kvlog
rm -rf $HOME/.lotus/repo.lock

lotus daemon