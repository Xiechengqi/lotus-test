package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/docker/go-units"
	paramfetch "github.com/filecoin-project/go-paramfetch"
)

const gateway = "https://proof-parameters.s3.cn-south-1.jdcloud-oss.com/ipfs/"
const gwEnv = "IPFS_GATEWAY"
const paramdir = "/var/tmp/filecoin-proof-parameters"
const dirEnv = "FIL_PROOFS_PARAMETER_CACHE"

func main() {
	var size = flag.String("size", "32GiB", "2KiB, 512MiB, 32GiB, 64GiB")
	var param = flag.String("json", "parameters.json", "param json file")
	var pathP = flag.String("path", "", "save param json file path")
	var gateP = flag.String("gate", "", "gateway path")
	flag.Parse()

	sectorSizeInt, err := units.RAMInBytes(*size)
	if err != nil {
		fmt.Println(err)
		flag.PrintDefaults()
		return
	}
	parametersJson, err := ioutil.ReadFile(*param)
	if err != nil {
		fmt.Println(err)
		flag.PrintDefaults()
		return
	}
	path := *pathP
	if path == "" {
		path = os.Getenv(dirEnv)
		if path == "" {
			path = paramdir
		}
	}
	os.Setenv(dirEnv, path)
	fmt.Println("path", os.Getenv(dirEnv))

	gate := *gateP
	if gate == "" {
		gate = os.Getenv(gwEnv)
		if gate == "" {
			gate = gateway
		}
	}
	os.Setenv(gwEnv, gate)
	fmt.Println("gateway", os.Getenv(gwEnv))

	sectorSize := uint64(sectorSizeInt)
	err = paramfetch.GetParams(context.Background(), parametersJson, sectorSize)
	if err != nil {
		fmt.Println("fetching proof parameters:", err)
		flag.PrintDefaults()
	}
}
