#/bin/bash

#lotus-miner init restore /root/storage_store/backup.cbor

cp /root/storage_store/config.toml $HOME/.lotusminer
cp /root/storage_store/storage.json $HOME/.lotusminer
cp /root/storage_store/token $HOME/.lotusminer
cp -r /root/storage_store/datastore $HOME/.lotusminer
cp -r /root/storage_store/keystore $HOME/.lotusminer
mkdir -p $HOME/.lotusminer/cache
mkdir -p $HOME/.lotusminer/unsealed
mkdir -p $HOME/.lotusminer/sealed

nohup lotus-miner run ${REDIS_OPTS} > miner.log 2>&1 &

sleep 10

lotus-miner storage attach --init --seal /root/.lotusminer
lotus-miner storage list

tail -f miner.log