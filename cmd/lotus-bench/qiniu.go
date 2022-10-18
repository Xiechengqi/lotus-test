package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func lastTreePaths(cacheDir string) []string {
	var ret []string
	paths, err := ioutil.ReadDir(cacheDir)
	if err != nil {
		return []string{}
	}
	for _, v := range paths {
		if !v.IsDir() {
			if strings.Contains(v.Name(), "tree-r-last") ||
				v.Name() == "p_aux" || v.Name() == "t_aux" {
				ret = append(ret, path.Join(cacheDir, v.Name()))
			}
		}
	}
	return ret
}

func submitQ(sbfs *basicfs.Provider, sector abi.SectorID) {
	cache := filepath.Join(sbfs.Root, storiface.FTCache.String(), storiface.SectorName(sector))
	seal := filepath.Join(sbfs.Root, storiface.FTSealed.String(), storiface.SectorName(sector))

	pathList := lastTreePaths(cache)
	pathList = append(pathList, seal)
	var reqs []*req
	for _, path := range pathList {
		fmt.Println("path ", path)
		reqs = append(reqs, newReq(path))
	}
	submitPaths(reqs)
}

func submitPathOut(paths []*req) {
	up := os.Getenv("UP_MONITOR")

	if up == "" {
		return
	}
	s, _ := json.Marshal(paths)
	sr := bytes.NewReader(s)
	r, err := http.DefaultClient.Post(up, "application/json", sr)
	if err != nil {
		fmt.Printf("submit path %+v err %s\n", paths, err.Error())
	} else {
		fmt.Printf("submit path %+v code %d\n", paths, r.StatusCode)
	}
}

func submitPaths(paths []*req) {
	up := os.Getenv("QINIU")

	if up == "" {
		return
	}
	conf2, err := operation.Load(up)
	if err != nil {
		log.Error("load config error", err)
		return
	}
	if conf2.Sim {
		submitPathOut(paths)
		return
	}
	uploader := operation.NewUploaderV2()
	for _, v := range paths {
		err := uploader.Upload(v.Path, v.Path)
		fmt.Printf("submit path %v err %v\n", v.Path, err)
		os.Remove(v.Path)
	}
}

type req struct {
	Path string `json:"path"`
}

func newReq(s string) *req {
	return &req{
		Path: s,
	}
}

func submitC1(s string, data []byte) error {
	up := os.Getenv("QINIU")

	if up == "" {
		return errors.New("no up")
	}
	conf2, err := operation.Load(up)
	if err != nil {
		log.Error("load config error", err)
		return err
	}
	if conf2.Sim {
		return errors.New("not support sim")
	}
	uploader := operation.NewUploaderV2()
	return uploader.UploadData(data, s)
}

func dowanloadC1(s string) ([]byte, error) {
	up := os.Getenv("QINIU")
	if up == "" {
		return nil, errors.New("no up")
	}
	conf2, err := operation.Load(up)
	if err != nil {
		log.Error("load config error", err)
		return nil, err
	}
	if conf2.Sim {
		return nil, errors.New("not support sim")
	}
	downloader := operation.NewDownloaderV2()
	return downloader.DownloadBytes(s)
}
