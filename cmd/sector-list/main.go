package main

import (
	"flag"
	"log"
	"path"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func main() {
	c := flag.String("c", "config.toml", "config file")
	root := flag.String("p", "home/fc", "prefix path")
	flag.Parse()

	conf, err := operation.Load(*c)
	if err != nil {
		log.Println("load conf failed", err, *c)
		return
	}

	l := operation.NewLister(conf)
	f := l.ListPrefix(*root)
	log.Println(f)

	sectors := loadSectors(f)
	log.Println(sectors)
}

func loadSectors(file []string) (ret []abi.SectorID) {
	for _, v := range file {
		x, err := stores.ParseSectorID(path.Base(v))
		if err == nil {
			ret = append(ret, x)
		}
	}
	return
}
