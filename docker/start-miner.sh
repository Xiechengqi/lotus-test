#/bin/bash

rm -rf $HOME/.lotusminer/data-transfer
rm -rf $HOME/.lotusminer/heapprof
rm -rf $HOME/.lotusminer/journal
rm -rf $HOME/.lotusminer/kvlog
rm -rf $HOME/.lotusminer/repo.lock

${TASK_SET} lotus-miner run ${POST_OPTS} ${REDIS_OPTS} ${ABILITY_OPTS}