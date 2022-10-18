package ffiwrapper

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/xerrors"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func readCachePath(cacheDir string) []string {
	var ret []string
	paths, err := ioutil.ReadDir(cacheDir)

	if err != nil {
		log.Infof("QINIU read cacheDir %s error %s", cacheDir, err)
		return []string{}
	}

	for _, v := range paths {
		if !v.IsDir() {
			if strings.Contains(v.Name(), "tree-r-last") ||
				v.Name() == "p_aux" || v.Name() == "t_aux" {
				ret = append(ret, v.Name())
			}
		}
	}
	return ret
}

func GenCacheFileList(ssize abi.SectorSize, sid abi.SectorID) (list []string) {
	list = append(list, "t_aux", "p_aux")
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		list = append(list, "sc-02-data-tree-r-last.dat")
	case 32 << 30:
		for i := 0; i < 8; i++ {
			list = append(list, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			list = append(list, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))
		}
	}
	return list
}

func BuildFixedPathUnsealedKey(sector abi.SectorID) string {
	storePath := os.Getenv(string(QiniuFeatureSingleSectorPath))
	return filepath.Join(storePath, storiface.FTUnsealed.String(), storiface.SectorName(sector))
}

func BuildFixedPathSealedKey(sector abi.SectorID) string {
	storePath := os.Getenv(string(QiniuFeatureSingleSectorPath))
	return filepath.Join(storePath, storiface.FTSealed.String(), storiface.SectorName(sector))
}

func BuildFixedPathCacheKey(sector abi.SectorID, cacheFile string) string {
	storePath := os.Getenv(string(QiniuFeatureSingleSectorPath))
	return filepath.Join(storePath, storiface.FTCache.String(), storiface.SectorName(sector), cacheFile)
}

func SubmitFixedPathSector(paths storiface.SectorPaths, sector abi.SectorID, uploadUnseal bool) error {
	//上传到固定路径
	if !QiniuFeatureEnabled(QiniuFeatureSingleSectorPath) {
		return errors.New("需要启用QINIU_STORE_PATH变量，指向永久保存目录，需要是绝对路径。例如 QINIU=/root/cfg.toml QINIU_FINALIZE_UPLOAD=true QINIU_STORE_PATH=/ceph ./lotus-worker")
	}

	var reqs = []*qiniuUpReq{}

	if uploadUnseal {
		reqs = append(reqs, &qiniuUpReq{
			Path: paths.Unsealed,
			Key:  BuildFixedPathUnsealedKey(sector),
		})
	}

	reqs = append(reqs, &qiniuUpReq{
		Path: paths.Sealed,
		Key:  BuildFixedPathSealedKey(sector),
	})

	cacheFiles := readCachePath(paths.Cache)

	for _, cacheFile := range cacheFiles {
		reqs = append(reqs, &qiniuUpReq{
			filepath.Join(paths.Cache, cacheFile),
			BuildFixedPathCacheKey(sector, cacheFile),
		})
	}
	return SubmitQiniuPaths(reqs, false)
}

func SubmitQiniuPaths(paths []*qiniuUpReq, removeLocalFile bool) error {
	up := os.Getenv("QINIU")
	if up == "" {
		return nil
	}
	uploader := operation.NewUploaderV2()

	if uploader == nil {
		return errors.New("QINIU upload init error")
	}

	for _, v := range paths {
		if _, err := os.Stat(v.Path); os.IsNotExist(err) {
			if IsQiniuFileExists(v.Key) {
				continue
			}
		}

		err := uploader.Upload(v.Path, v.Key)
		log.Infof("QINIU : submit path=%v err=%v", v, err)
		if err != nil {
			return err
		}
		if removeLocalFile {
			os.Remove(v.Path)
		}
	}
	return nil
}

func qiniuCleanSeal(paths storiface.SectorPaths, keepUnsealed bool) error {
	//预检查seal文件是否上传
	keySealed := BuildFixedPathSealedKey(paths.ID)
	if !IsQiniuFileExists(keySealed) {
		return xerrors.Errorf("QINIU sealed file %w does not exist, skip clean", paths.ID, keySealed)
	}
	if keepUnsealed {
		//预检查unseal文件是否上传
		keyUnsealed := BuildFixedPathUnsealedKey(paths.ID)
		if !IsQiniuFileExists(keyUnsealed) {
			return xerrors.Errorf("QINIU unsealed file %w %s does not exist, skip clean", paths.ID, keyUnsealed)
		}
	}

	cacheFiles := readCachePath(paths.Cache)

	for _, cacheFile := range cacheFiles {
		//预检查cache文件是否上传
		keyCache := BuildFixedPathCacheKey(paths.ID, cacheFile)
		if !IsQiniuFileExists(keyCache) {
			return xerrors.Errorf("QINIU cache file %w %s %s does not exist, skip clean", paths.ID, cacheFile, keyCache)
		}
	}

	if localExists(paths.Sealed) {
		log.Infof("QINIU clean local path sealed %s", paths.Sealed)
		if err := os.RemoveAll(paths.Sealed); err != nil {
			return err
		}
	}

	if localExists(paths.Cache) {
		log.Infof("QINIU clean local path cache %s", paths.Cache)
		if err := os.RemoveAll(paths.Cache); err != nil {
			return err
		}
	}

	if keepUnsealed && localExists(paths.Unsealed) {
		log.Infof("QINIU clean local path unsealed %s", paths.Unsealed)
		if err := os.RemoveAll(paths.Unsealed); err != nil {
			return err
		}
	}
	return nil
}

func localExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func QiniuUploadByMiner(pathx string) error {
	// 此处实现从worker获取文件并上传逻辑，线上环境打开这段注释
	var filePathWalkDir = func(root string) ([]string, error) {
		var files []string
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				files = append(files, path)
			}
			return nil
		})
		return files, err
	}

	fileList, err := filePathWalkDir(pathx)
	if err != nil {
		return xerrors.Errorf("scan path of %s failed", pathx)
	}

	var reqs []*qiniuUpReq

	for _, v := range fileList {
		reqs = append(reqs, NewQiniuUpReq(v, v))
	}

	return SubmitQiniuPaths(reqs, true)
}

type qiniuUpReq struct {
	Path string
	Key  string
}

func NewQiniuUpReq(path, key string) *qiniuUpReq {
	return &qiniuUpReq{path, key}
}

func QiniuLoadSectors(prefix string) []string {
	log.Infof("QINIU load sectors with prefix: %s", prefix)
	if !strings.Contains(prefix, storiface.FTSealed.String()) {
		prefix = path.Join(prefix, storiface.FTSealed.String())
	}
	lister := operation.NewListerV2()
	files := lister.ListPrefix(strings.TrimPrefix(prefix, "/"))
	log.Debugf("QINIU loaded sectors: %s", files)
	return files
}

const fetchTempSubdir = "fetching"

func CheckFetching(root string, fullPath string) bool {
	full := strings.TrimPrefix(fullPath, "/")
	root1 := strings.TrimPrefix(root, "/")
	return strings.HasPrefix(full, filepath.Join(root1, fetchTempSubdir))
}

func FindSealedInPaths(paths []string, sid abi.SectorID) int {
	fullPaths := make([]string, len(paths))
	for i, path := range paths {
		fullPaths[i] = filepath.Join(path, storiface.FTSealed.String(), storiface.SectorName(sid))
	}
	return anyQiniuFileExists(fullPaths)
}

func FindCachePauxInPaths(paths []string, sid abi.SectorID) int {
	fullPaths := make([]string, len(paths))
	for i, path := range paths {
		fullPaths[i] = filepath.Join(path, storiface.FTCache.String(), storiface.SectorName(sid), "p_aux")
	}
	return anyQiniuFileExists(fullPaths)
}

func IsQiniuFileExists(fullPath string) bool {
	lister := operation.NewListerV2()

	//lister.Stat功能需要0.1.23之后引入，所以暂时使用ListStat功能替代
	stats := lister.ListStat([]string{strings.TrimPrefix(fullPath, "/")})
	if len(stats) == 0 {
		return false
	}
	return stats[0].Size >= 0
}

func anyQiniuFileExists(fullPaths []string) int {
	lister := operation.NewListerV2()
	newFullPaths := make([]string, len(fullPaths))
	for i, fullPath := range fullPaths {
		newFullPaths[i] = strings.TrimPrefix(fullPath, "/")
	}

	stats := lister.ListStat(newFullPaths)
	for i := range stats {
		if stats[i].Size >= 0 {
			return i
		}
	}
	return -1
}

var sectorSealedRoot = map[abi.SectorID]string{}
var sectorSealedRootMutex = sync.Mutex{}

//window post增加路径cache
//调用处1：./lotus-miner proving check会调用
//调用处2：window post调用链：checkProvable(faults.go)剔除所有坏扇区,将好扇区放在加入cache
func AddWindowPostSectorSealedRoot(sid abi.SectorID, root string) {
	sectorSealedRootMutex.Lock()
	sectorSealedRoot[sid] = root
	sectorSealedRootMutex.Unlock()
}

//此处用作window post 路径的 cache，减少多次远程调用调用
func WindowPostSectorSealedRoot(sid abi.SectorID) string {
	sectorSealedRootMutex.Lock()
	root := sectorSealedRoot[sid]
	sectorSealedRootMutex.Unlock()
	return root
}

var sectorCacheRoot = map[abi.SectorID]string{}
var sectorCacheRootMutex = sync.Mutex{}

//window post增加路径cache
//调用处1：./lotus-miner proving check会调用
//调用处2：window post调用链：checkProvable(faults.go)剔除所有坏扇区,将好扇区放在加入cache
func AddWindowPostSectorCacheRoot(sid abi.SectorID, root string) {
	sectorCacheRootMutex.Lock()
	sectorCacheRoot[sid] = root
	sectorCacheRootMutex.Unlock()
}

//此处用作window post 路径的 cache，减少多次远程调用调用
func WindowPostSectorCacheRoot(sid abi.SectorID) string {
	sectorCacheRootMutex.Lock()
	root := sectorCacheRoot[sid]
	sectorCacheRootMutex.Unlock()
	return root
}

//以下代码用于变量开关
type QiniuFeature string

const QiniuFeatureMinerUpload QiniuFeature = "QINIU_MINER_UPLOAD"
const QiniuFeatureFinalizeUpload QiniuFeature = "QINIU_FINALIZE_UPLOAD"
const QiniuFeatureFinalizeUploadAutoClean QiniuFeature = "QINIU_FINALIZE_UPLOAD_AUTOCLEAN"
const QiniuFeatureSearchMultiSectorPath QiniuFeature = "QINIU_AUTOCONFIG_SECTOR_PATH"
const QiniuFeatureMultiSectorPaths QiniuFeature = "QINIU_MULTIPLE_PATH"
const QiniuFeatureSingleSectorPath QiniuFeature = "QINIU_STORE_PATH"
const QiniuFeatureLoadSectors QiniuFeature = "QINIU_LOAD_SECTORS"

func QiniuFeatureEnabled(f QiniuFeature) bool {
	if !UseQiniu() {
		return false
	}
	if os.Getenv(string(f)) == "" ||
		os.Getenv(string(f)) == "FALSE" ||
		os.Getenv(string(f)) == "false" ||
		os.Getenv(string(f)) == "0" {
		return false
	}
	return true
}

func UseQiniu() bool {
	return os.Getenv("QINIU") != ""
}

func QiniuStorePath() string {
	return os.Getenv(string(QiniuFeatureSingleSectorPath))
}

//以下代码允许传入扇区路径
func init() {
	paths := QiniuMultipleSectorPath()
	for _, v := range paths {
		AddWindowPostPath(v)
	}
}

//该变量用于windowpost/winningpost（主要解决winningpost找不到路径的问题）
func QiniuMultipleSectorPath() []string {
	return strings.Split(os.Getenv(string(QiniuFeatureMultiSectorPaths)), ":")
}

var windowPostPaths []string
var windowPostPathsLock sync.Mutex

func AddWindowPostPath(path string) {
	windowPostPathsLock.Lock()
	if !hasWindowPath(path) {
		windowPostPaths = append(windowPostPaths, path)
	}
	windowPostPathsLock.Unlock()
}

func hasWindowPath(path string) (ret bool) {
	ret = false
	for _, v := range windowPostPaths {
		if strings.TrimSuffix(v, "/") == strings.TrimSuffix(path, "/") {
			ret = true
			break
		}
	}
	return ret
}

func GetWindowPostPath() []string {
	return windowPostPaths
}

//判断是否为winningpost。
func IsWinningPost(proof abi.RegisteredPoStProof) bool {
	var RegisteredPoStProof_StackedDrgWinning2KiBV1 = abi.RegisteredPoStProof(0)
	var RegisteredPoStProof_StackedDrgWinning8MiBV1 = abi.RegisteredPoStProof(1)
	var RegisteredPoStProof_StackedDrgWinning512MiBV1 = abi.RegisteredPoStProof(2)
	var RegisteredPoStProof_StackedDrgWinning32GiBV1 = abi.RegisteredPoStProof(3)
	var RegisteredPoStProof_StackedDrgWinning64GiBV1 = abi.RegisteredPoStProof(4)
	if proof == RegisteredPoStProof_StackedDrgWinning2KiBV1 ||
		proof == RegisteredPoStProof_StackedDrgWinning8MiBV1 ||
		proof == RegisteredPoStProof_StackedDrgWinning512MiBV1 ||
		proof == RegisteredPoStProof_StackedDrgWinning32GiBV1 ||
		proof == RegisteredPoStProof_StackedDrgWinning64GiBV1 {
		return true
	}
	return false
}

func IsWindowPost(proof abi.RegisteredPoStProof) bool {
	var RegisteredPoStProof_StackedDrgWindow2KiBV1 = abi.RegisteredPoStProof(5)
	var RegisteredPoStProof_StackedDrgWindow8MiBV1 = abi.RegisteredPoStProof(6)
	var RegisteredPoStProof_StackedDrgWindow512MiBV1 = abi.RegisteredPoStProof(7)
	var RegisteredPoStProof_StackedDrgWindow32GiBV1 = abi.RegisteredPoStProof(8)
	var RegisteredPoStProof_StackedDrgWindow64GiBV1 = abi.RegisteredPoStProof(9)
	if RegisteredPoStProof_StackedDrgWindow2KiBV1 == proof ||
		RegisteredPoStProof_StackedDrgWindow8MiBV1 == proof ||
		RegisteredPoStProof_StackedDrgWindow512MiBV1 == proof ||
		RegisteredPoStProof_StackedDrgWindow32GiBV1 == proof ||
		RegisteredPoStProof_StackedDrgWindow64GiBV1 == proof {
		return true
	}
	return false
}
